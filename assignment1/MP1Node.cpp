/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }
    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	//int id = *(int*)(&memberNode->addr.addr);
	//int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 500;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    memberNode->inGroup = false;
    memberNode->bFailed = true;
    return 0;
}

Address * MP1Node::ToAddress(int id, short port) {
    Address *address = new Address();
    memcpy(&address->addr[0], &id, sizeof(int));
    memcpy(&address->addr[4], &port, sizeof(short));
    //cout<<"Address is ";
    //for (int i=0; i<6; i++) {
    //    cout<<(int)address->addr[i]<<" ";
    //}
    //for(int j=4; j<6; j++)
        //cout<<*(short *)&address->addr[4];
    //cout<<endl;
    return address;
}
/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }
    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

bool MP1Node::outdatedEntry(int id) {
    if (failedID.size() < 1)
        return false;
    for (vector<int>::iterator i=failedID.begin(); i!=failedID.end(); i++) {
        if (*i == id)
            return true;
    }
    return false;
}

void MP1Node::checkAndUpdateEntry(Address *responder, long heartbeat, long timestamp) {
    vector<MemberListEntry>::iterator it;
    int id=*(int *)&responder->addr[0];
    short port=*(short *)&responder->addr[4];
    bool isPresent = false;

    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
        if (id == it->id && port == it->port ) {
            isPresent = true;
            if (heartbeat > it->heartbeat) {
                it->setheartbeat(heartbeat);
                it->settimestamp(this->par->getcurrtime());
            }
        }
    }
    if (!isPresent) {
        updateMPTable(responder, heartbeat);
    }
    return;
}

void MP1Node::updateMPTable(Address *responder, long heartbeat) {
    int i=0;
    for(i = 0; i < 6; i++){
        if(responder->addr[i] != memberNode->addr.addr[i])
            break;
    }
    if (i==6)
        return;
    //if (responder == &memberNode->addr)
    //    return ;
    int id = (int)responder->addr[0];
    short port = (short)responder->addr[4];
    MemberListEntry entry(id, port, heartbeat, this->par->getcurrtime());
    memberNode->memberList.push_back(entry);
    //cout<<"Adding "<<*(int *)&responder->addr[0]<<" in "<<*(int *)&memberNode->addr.addr[0]<<endl;
    log->logNodeAdd(&memberNode->addr, responder);
    return;
}

void MP1Node::sendJoinRep(Address *responder) {
    int listSize = memberNode->memberList.size(), offset=1;
    size_t gossipSize = sizeof(MessageHdr) + sizeof(listSize) + (listSize * (sizeof(int)
        + sizeof(short) + sizeof(long) + sizeof(long) ));
    MessageHdr *reply = (MessageHdr *) malloc(gossipSize * sizeof(char));

    reply->msgType = JOINREP;
    memcpy((char *)(reply+offset), &listSize, sizeof(int));
    offset += sizeof(int);
    for (vector<MemberListEntry>::iterator it=memberNode->memberList.begin(); it!=memberNode->memberList.end(); it++) {
        memcpy((char *)(reply)+offset, &it->id, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)(reply)+offset, &it->port, sizeof(short));
        offset += sizeof(short);
        memcpy((char *)(reply)+offset, &it->heartbeat, sizeof(long));
        offset += sizeof(long);
        memcpy((char *)(reply)+offset, &it->timestamp, sizeof(long));
        offset += sizeof(long);
    }
    //cout<<"id is "<<memberNode->myPos->id<<" and original is "<<(int)(memberNode->addr.addr[0])<<endl;    
    emulNet->ENsend(&memberNode->addr, responder, (char *)reply, gossipSize);
    free(reply);
    return;
}
/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *response;
    Address *responder;

    response = (MessageHdr *)data;
    responder = (Address *)((char *)(response+1));
    if (response->msgType==JOINREQ) {
        long heartbeat = *(long *)((char *)(response+1) + 1 + sizeof(memberNode->addr.addr));
        //cout<<heartbeat<<endl;
        updateMPTable(responder, heartbeat);
        sendJoinRep(responder);
    }
    else if (response->msgType==JOINREP) {
        memberNode->inGroup = true;
        int tableSize = (int)*((char *)(response+1)), id, offset = 1+sizeof(int);
        short port;
        long heartbeat;
        for (int i = 0; i < tableSize; ++i) {
            id = *(int *)((char *)(response) + offset);
            offset += sizeof(int);
            port = *(short *)((char *)(response) + offset);
            offset += sizeof(short);
            heartbeat = *(long *)((char *)(response) + offset);
            offset += sizeof(long);
            //:ALERT: timestamp = *(long *)((char *)(response) + offset);
            offset += sizeof(long);

            updateMPTable(ToAddress(id, port), heartbeat);
        }
    }
    else if (response->msgType==GOSSIP) {
        if (!memberNode->inGroup)
            return false;
        int tableSize = (int)*((char *)(response+1)), id, offset = 1+sizeof(int);
        short port;
        long timestamp, heartbeat;
        for (int i = 0; i < tableSize; ++i) {
            id = *(int *)((char *)(response) + offset);
            offset += sizeof(int);
            port = *(short *)((char *)(response) + offset);
            offset += sizeof(short);
            heartbeat = *(long *)((char *)(response) + offset);
            offset += sizeof(long);
            timestamp = *(long *)((char *)(response) + offset);
            //cout<<timestamp
            offset += sizeof(long);
            checkAndUpdateEntry(ToAddress(id, port), heartbeat, timestamp);
        }
    }
    else {
        printf("Unrecognized message\n");
        return false;
    }
    return 0;
}

void MP1Node::sendGossip(int targetID) {
    int listSize = memberNode->memberList.size() - failedID.size(), offset=1;
    size_t gossipSize = sizeof(MessageHdr) + sizeof(listSize) + (listSize * (sizeof(int)
        + sizeof(short) + sizeof(long) + sizeof(long) ));
    MessageHdr *reply = (MessageHdr *) malloc(gossipSize * sizeof(char));

    reply->msgType = GOSSIP;
    memcpy((char *)(reply+offset), &listSize, sizeof(int));
    offset += sizeof(int);
    for (vector<MemberListEntry>::iterator it=memberNode->memberList.begin(); it!=memberNode->memberList.end(); it++) {
        if (find(failedID.begin(), failedID.end(), it->id) != failedID.end()) {
            continue;
        }
        memcpy((char *)(reply)+offset, &it->id, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)(reply)+offset, &it->port, sizeof(short));
        offset += sizeof(short);
        memcpy((char *)(reply)+offset, &it->heartbeat, sizeof(long));
        offset += sizeof(long);
        memcpy((char *)(reply)+offset, &it->timestamp, sizeof(long));
        offset += sizeof(long);
    }

    emulNet->ENsend(&memberNode->addr,
        ToAddress(memberNode->memberList[targetID].id, memberNode->memberList[targetID].port),
        (char *)reply, gossipSize);
    free(reply);
    return;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    //cout<<*(int *)&memberNode->addr.addr[0]<<endl;
    // Increment timestamp
    //printTable();
    memberNode->memberList.begin()->setheartbeat(memberNode->memberList.begin()->getheartbeat()+1);
    memberNode->memberList.begin()->settimestamp(par->getcurrtime());
    //cout<<"id is "<<memberNode->memberList.begin()->id<<" and original is "<<(int)(memberNode->addr.addr[0])<<endl;
    //if (*(int *)&memberNode->addr.addr[0] == INSPECT_NODE)
    //    printTable();
    int node1, node2, node3;
    int totalNodes = memberNode->memberList.size();
    if (totalNodes > 3) {
        node3 = node2 = node1 = rand() % (memberNode->memberList.size()-1) + 1;
        while(node2 == node1){
            node2 = rand() % (memberNode->memberList.size()-1) + 1;
        }
        while(node3 == node2 || node3 == node1){
            node3 = rand() % (memberNode->memberList.size()-1) + 1;
        }
        sendGossip(node1);sendGossip(node2);sendGossip(node3);
    }

    //Check for timed out nodes
	for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end();) {
        int time_diff = par->getcurrtime() - it->timestamp;
        if (time_diff > TFAIL) {
            softDelete(it);
        }
        if (time_diff > TREMOVE) {
            it = hardDelete(it);
            continue;
        }
        else {
            it++;
        }
    }
    return;
}

void MP1Node::printTable() {
    cout<<"---------Table for "<<(int)*(int *)&memberNode->addr.addr<<"---------"<<endl;
    cout<<"|---Id---|---Port---|---Heartbeat---|---Timestamp---|"<<endl;
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); ++it) {
        cout<<"    "<<it->id<<"           "<<it->port<<"           "<<it->heartbeat<<"              "<<it->timestamp<<endl;
    }
}

void MP1Node::softDelete(vector<MemberListEntry>::iterator it) {
    //cout<<"soft delete of "<<it->id<<" by "<<*(int *)&memberNode->addr.addr[0]<<endl;
    std::vector<int>::iterator i;
    i=find(failedID.begin(), failedID.end(), it->id);
    if (i == failedID.end())
        failedID.push_back(it->id);
    /*if (*(int *)&memberNode->addr.addr[0] == INSPECT_NODE)
        cout <<"deleting "<<it->id<<endl;
    if (failedID.size()<1) {
        //cout<<"Pushed "<<it->id<<" by "<<*(int *)&memberNode->addr.addr[0]<<endl;
        //cout<<"Removing "<<it->id<<endl;
        memberNode->memberList.erase(it);
        failedID.push_back(it->id);
        log->logNodeRemove(&memberNode->addr, ToAddress(it->id, it->port));
        return;
    }

    for (i=failedID.begin(); i!=failedID.end(); i++) {
        if (*(int *)&memberNode->addr.addr[0] == INSPECT_NODE)
            cout << *i <<",";
        if (*i == it->id) {
            //cout<<"Found "<<*i<<endl;
            //failedID.erase(i);
            break;
        }
    }*/
    /*if (i==failedID.end()) {
        //cout<<"Pushed "<<it->id<<" by "<<*(int *)&memberNode->addr.addr[0]<<endl;
        failedID.push_back(it->id);
        //memberNode->memberList.erase(it);
        //log->logNodeRemove(&memberNode->addr, ToAddress(it->id, it->port));
    }
    else {
        //Already present
    }*/
    return;
}

std::vector<MemberListEntry>::iterator MP1Node::hardDelete(vector<MemberListEntry>::iterator it) {
    //cout<<"Removing "<<it->id<<"from table of "<<*(int *)&memberNode->addr.addr[0]<<endl;
    //log->logNodeRemove(&memberNode->addr, ToAddress(it->id, it->port));
    //cout<<"hard delete of "<<it->id<<" by "<<*(int *)&memberNode->addr.addr[0]<<endl;
    /*cout<<"status of "<<*(int *)&memberNode->addr.addr[0]<<" while deleting "<<it->id<<endl;
    for (std::vector<int>::iterator i=failedID.begin(); i!=failedID.end(); i++) {
        cout<<*i<<", ";
    }
    cout<<endl;*/
    log->logNodeRemove(&memberNode->addr, ToAddress(it->id, it->port));
    failedID.erase(find(failedID.begin(), failedID.end(), it->id));
    it = memberNode->memberList.erase(it);
    /*cout<<"searching "<<it->id<<" by "<<*(int *)&memberNode->addr.addr[0]<<endl;
    cout<<"Couldn't find!"<<it->id<<endl;*/
    return it;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    int id = *(int *)&memberNode->addr.addr[0];
    short port = *(short *)&memberNode->addr.addr[4];
    MemberListEntry entry(id, port, memberNode->heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(entry);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
