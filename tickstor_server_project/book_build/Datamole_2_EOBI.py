from BookData import *
import sys

class EOBI:
    def __init__(self,levels,date_string,logger):
        self.__log=logger
        if levels<1:
            raise Exception("Levels must be 1 or more")
        
        self.bookData=ByOrderBookData(levels,date_string)
        
        self.__snapshot_cycle=-1  # keeps track of where we are in the snapshot cycle
        self.__new_snapshot_cycle=-1  # this is to handle a snapshot cycle that comes before the last one ends.
        self.__snapshot_queue=[]  # for storing out of sequence snapshot cycle ticks
        self.__new_snapshot_queue=[] # for storing out of sequence snapshot cycle ticks for the new cylce when the old one is not done.
        self.__last_header=None  #this is the last eobi_header we use this to check for the end of the cycle
        self.__previous_tick=None #this is the last tick... we use the to check for the end of the cycle
        self.__last_seqnum=0  # The last sequence number that the snapshot cycle is looking at.
        self.__uid=None #this is the current uid in the snapshot cycle
        self.snapshot_products={} #we use this to make sure we only snapshot each product once.
        
        self.__missing_seqnum={}
        self.__last_seqnum={}
        
    def findFirstMsgseqnum(self,date,ticks):  #ticks is a list of DataMole ticks
        if date==self.bookData.date:
            for tick in ticks: # read file line by line
                if tick.name=='eobi_header' or tick.name=="eobi_13600_product_summary" or tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order":
                    continue       
                uid=tick.values['secid']
                msgseqnum=int(tick.values['msgseqnum'])
                #As long as the message is not a snapshot message, this is the first incremental we can see, 
                # and hence when we subscribed to the multicast channel.
                if uid not in self.bookData.product_sequence_numbers:
                    self.bookData.init_book(uid,msgseqnum)
                    self.__log.info("Found first product:  {0} {1} {2}".format(uid,tick.name,msgseqnum))
        else:
            self.__log.fatal("Tried to parse on the wrong date: {0} {1}".format(self.booData.date,date))
            
    def parseForMissingSeqnum(self,date,ticks):
        #TODO:  I think these are here because we are not parsing all of the messages, but we need to verify this.
        if date==self.bookData.date:
            for tick in ticks: # read file line by line
                if tick.name=='eobi_header' or tick.name=="eobi_13600_product_summary" or tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order":
                    continue       
                uid=tick.values['secid']
                msgseqnum=int(tick.values['msgseqnum'])
                #As long as the message is not a snapshot message, this is the first incremental we can see, 
                # and hence when we subscribed to the multicast channel.
                if uid not in self.__last_seqnum: self.__last_seqnum[uid]=self.bookData.product_sequence_numbers[uid]-1
                if uid not in self.__missing_seqnum: self.__missing_seqnum[uid]=[]
                if msgseqnum>self.__last_seqnum[uid]+1:
                    #gap
                    fill=self.__last_seqnum[uid]+1
                    while fill<msgseqnum:
                        self.__missing_seqnum[uid].append(fill)
                        fill+=1
                if msgseqnum<=self.__last_seqnum[uid]:
                    #out of sequence coming in
                    self.__missing_seqnum[uid].remove(msgseqnum)
                self.__last_seqnum[uid]=msgseqnum
        else:
            self.__log.fatal("Tried to parse on the wrong date: {0} {1}".format(self.booData.date,date))
        
    def computeSnapshotData(self,date,ticks):
        if date==self.bookData.date:
            for i in range(len(ticks)):
                tick=ticks[i]
                # This will work on the snapshot channel only....
                if tick.name=='eobi_header':
                    # Check if the previous snapshot message was a Complete message indicating the end of the snapshot cycle.
                    if self.__previous_tick is not None and (self.__previous_tick.name=="eobi_13600_product_summary" or self.__previous_tick.name=="eobi_13601_instrument_summary_header_body" or self.__previous_tick.name=="eobi_13602_snapshot_order"):
                        if self.__last_header is not None and int(self.__last_header.values["CompletionIndicator"])==1:
                            #End of snapshot cyle.  Clean up.
                            self.__log.debug("Clear End: " + tick.name)
                            self.__snapshot_queue=[]
                            self.__snapshot_cycle=-1
                            self.__last_seqnum=0
                    self.__last_header=tick 
                count =0
                # This is the case where we get the start too early.
                if self.__snapshot_cycle==-1:
                    while count<len(self.__new_snapshot_queue):
                        self.__log.debug("Running New Cycle")
                        qtick= self.__new_snapshot_queue.pop(0)
                        self.__readTickForSnapshot(qtick,True)
                        count+=1
                    self.__new_snapshot_queue=[]
                    self.__new_snapshot_cycle=-1
                else:
                    while count<len(self.__snapshot_queue):
                        qtick= self.__snapshot_queue.pop(0)
                        self.__readTickForSnapshot(qtick,True)
                        count+=1
                self.__readTickForSnapshot(tick)
                self.__previous_tick=tick
        else:
            self.__log.fatal("Tried to parse on the wrong date: {0} {1}".format(self.booData.date,date))

    def __readTickForSnapshot(self,tick,recheck=False):
        if tick.name=="eobi_13600_product_summary" or tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order":
            msgseqnum=int(tick.values['msgseqnum'])
            # If we are already in a snapshot cycle and get the start of a new one without an end one, we have out of sequence packets.
            if self.__snapshot_cycle>-0 and msgseqnum==0 and tick.name=="eobi_13600_product_summary":
                self.__new_snapshot_queue.append(tick)
                self.__log.debug("Too Early: " + str(msgseqnum) + " " + str(len(self.__new_snapshot_queue)))
            elif self.__snapshot_cycle>=0 and msgseqnum==self.__new_snapshot_cycle+1 and (tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order"):
                self.__new_snapshot_cycle=msgseqnum
                self.__new_snapshot_queue.append(tick)
                self.__log.debug("Too Early: " + str(msgseqnum) + " " + str(len(self.__new_snapshot_queue)))
            # A product summary message with a sequence number of 0 starts the snapshot cycle
            elif self.__snapshot_cycle==-1 and msgseqnum==0 and tick.name=="eobi_13600_product_summary":
                self.__last_seqnum=int(tick.values['lastmegseqnumprocessed'])
                self.__snapshot_cycle=0
                self.__log.info("Product Summary: " + str(self.__last_seqnum) + " " + str(len(self.__snapshot_queue)))
            #  If we are in a snapshot cycle we can get Instrument Summary messages and then Order Messages to build the book
            elif self.__snapshot_cycle>=0 and msgseqnum==self.__snapshot_cycle+1 and (tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order"):
                self.__snapshot_cycle=msgseqnum    
                # A summary message is always before the order messages
                if (tick.name=="eobi_13601_instrument_summary_header_body"):
                    self.__uid=None #clear the previous UID
                    uid=tick.values['securityid']
                    self.__log.debug("InstMsg: " + str(msgseqnum) + " Q: " + str(len(self.__snapshot_queue))+ " uid:" + str(uid)+ " last:" + str(self.__last_seqnum) + " incr: " + str(self.bookData.product_sequence_numbers[uid]))
                    # We only want to consider the security if:
                    # 1) We have not used the product before
                    # 2) The sequence number is one that we can grow the incremental from 
                    if uid not in self.snapshot_products.keys() and uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]<=self.__last_seqnum:
                        self.__uid=uid
                        self.snapshot_products[self.__uid]=self.__last_seqnum
                        self.bookData.product_sequence_numbers[self.__uid]=self.__last_seqnum
                        self.__log.info("New Product: " + str(self.__uid) + " " + str(len(self.__snapshot_queue)))
                if (tick.name=="eobi_13602_snapshot_order"):
                    self.__log.debug("OrdMsg: " + str(msgseqnum) + " " + str(len(self.__snapshot_queue)))
                    # self.__uid is set by the Instrument Summary message
                    if self.__uid is not None and self.__uid in self.snapshot_products.keys():
                        oid=tick.values['trdregTStimepriority'] 
                        side = int(tick.values['side'])-1
                        qty= int(tick.values['displayqty'])
                        price=float(tick.values["price"])/100000000.0 #TODO: I am not sure if this is right to divide by 100000000
                        self.__new_order(self.__uid,oid,side,price,qty,tick.timestamp)
                        self.__log.info("Order " + str(msgseqnum) + " " + str(oid)+ " " + str(side) + " " +str(price) + " " + str(qty ))
            # If we are in a snapshot cycle but the sequence nubmers do not add up, then we have an out of sequence packet that we need to reorder
            elif self.__snapshot_cycle>=0 and msgseqnum!=self.__snapshot_cycle+1 and (tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order"):
                # TODO: There is some funny stuff going on with this where I am up to packet 5000 and then I get packet 1200 and move up to and past 5000.  WTF IS THAT?
                self.__snapshot_queue.append(tick)
                if not recheck: self.__log.debug("Out of sequence Snapshot: " + str(msgseqnum) + " " + str(len(self.__snapshot_queue)))
            elif self.__snapshot_cycle>=0:
                pass #Something has gone wrong if we see this I think
                self.__log.warn(tick.name + " " + str(msgseqnum)+ " "  + str(self.__snapshot_cycle))
            else:
                #snapshot cycle has not started we jumped onto the multicast in the middle.... throw these away
                pass
                    
    def calcEOBI(self,date,ticks):  #ticks is a list of DataMole ticks
        if date==self.bookData.date:
            for tick in ticks: # read file line by line
                if tick.name=='eobi_header' or tick.name=="eobi_13600_product_summary" or tick.name=="eobi_13601_instrument_summary_header_body" or tick.name=="eobi_13602_snapshot_order":
                    continue
                uid=tick.values['secid']
                msgseqnum=int(tick.values['msgseqnum'])
                #cycle through the missing sequence numbers
                if len(self.__missing_seqnum[uid])>0:
                    while msgseqnum>self.__missing_seqnum[uid][0]:
                        seq=self.__missing_seqnum[uid].pop(0)
                        self.bookData.product_sequence_numbers[uid]=seq
                # Go through stored data first to see if we are ready to 
                # parse data that was previously stored because we did not
                # have a snaphot or it had been out of sequence
                for suid in self.bookData.product_stored_data.keys():
                    count=len(self.bookData.product_stored_data[suid])
                    while count>0:
                        st_tick=self.bookData.product_stored_data[suid].pop(0)
                        if suid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[suid]>=msgseqnum:
                            pass  #this is the A/B feed case
                        elif suid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[suid]+1==msgseqnum:
                            self.bookData.product_sequence_numbers[suid]+=1
                            self.__parse_tick(st_tick)
                        else:
                            self.bookData.product_stored_data[suid].append(st_tick)
                        count-=1
                    if len(self.bookData.product_stored_data[suid])==0:
                        self.bookData.product_stored_data.pop(suid)
                # After we have read all of the previously stored data we need to then process the next tick.
                if uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]>=msgseqnum:
                    continue  #this is the A/B feed case or that we do not have the snapshot yet.
                elif uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]+1==msgseqnum:
                    self.bookData.product_sequence_numbers[uid]+=1
                    self.__parse_tick(tick)
                else:
                    if uid not in self.bookData.product_stored_data:
                        self.bookData.product_stored_data[uid]=[]
                    self.bookData.product_stored_data[uid].append(tick)
                    self.__log.info("OutOfSeq: {0} LastSeq: {1} CurrSeq: {2} Len: {3}".format(uid,self.bookData.product_sequence_numbers[uid],msgseqnum,len(self.bookData.product_stored_data[uid])))
                    #TODO:  We need to check that we had a clean parse... once we are completely done we need to check what is in self.bookData.product_sequence_numbers
        else:
            self.__log.fatal("Tried to parse on the wrong date: {0} {1}".format(self.booData.date,date))
            
    ############################################################
    #
    #   Parse Functions
    #
    ############################################################        
    def __parse_tick(self,tick):
        uid=tick.values['secid']
        msgseqnum=int(tick.values['msgseqnum'])
        #we ignore  'eobi_13504_top_of_book'    
        if uid not in self.bookData.obooks:
            self.bookData.init_book(uid,msgseqnum)
        if tick.name=='eobi_13103_order_mass_del':
            obooks[uid] = ({}, {})
            # TODO: Should I output the clear?
            #self.bookData.printBook("C",uid,int(tick.timestamp),int(tick.timestamp))
        elif tick.name== 'eobi_13201_trade_report':
            pass  # Need to handle this to report the trades faster....
        elif tick.name=='eobi_13202_exec_summary':
            pass  # Need to handle this to report the trades faster....
        elif tick.name=='eobi_13502_cross_request':
            pass  # Need to output this for signals...
        elif tick.name=='eobi_13503_quote_request':
            pass  # Need to output this for signals...
        else:
            recv= int(tick.timestamp)
            side= int(tick.values["side"])-1
            oid = int(tick.values["trdregTStimepriority"])
            price=float(tick.values["price"])/100000000.0 #TODO: I am not sure if this is right to divide by 100000000
            
            if tick.name=='eobi_13100_ord_add':
                exch= int(tick.values["trdregtstimein"])
                qty = int(tick.values["qty"])
                self.__new_order(uid,oid,side,price,qty,exch)
                self.bookData.printBook("A",uid,exch,recv)               
            elif tick.name=='eobi_13101_order_mod':
                exch= int(tick.values["trdregtstimein"])
                qty = int(tick.values["DisplayQty"])
                prev = int(tick.values["PrevPrice"])
                old_oid = int(tick.values["TrdRegTSPrevTime-Pri"])
                if oid==old_oid:
                    # Queue priority not lost, qty reduced
                    self.__modify_order(uid, oid, side, price, qty, exch)
                    self.bookData.printBook("M",uid,exch,recv)
                else:
                    # Queue priority lost, qty increased or price changed
                    self.__delete_order(uid, old_oid, side, prev)
                    self.__new_order(uid,oid,side,price,qty,exch) 
                    self.bookData.printBook("R",uid,exch,recv)               
            elif tick.name=='eobi_13102_order_del':
                exch= int(tick.values["trdregtstimein"])
                self.__delete_order(uid, oid, side, 0)
                self.bookData.printBook("D",uid, exch,recv)                   
            elif tick.name=='eobi_13104_13105_order_exec':
                qty = int(tick.values["qty"])
                #TODO: this is not right... I need the TransactTime
                self.bookData.printTrade(uid,recv,recv,price,qty)            
            elif tick.name=='eobi_13106_order_mod_same_priority':
                exch= int(tick.values["trdregtstimein"])
                qty = int(tick.values["qty"])
                self.__modify_order(uid,oid,side,price,-qty,exch)
                self.bookData.printBook("M",uid,exch,recv)               
            else:
                pass    

    ############################################################
    #
    #   Book Functions
    #
    ############################################################

    # find index of tuple in a list of tuples where the first value is the key
    # [ (key1, v1), (key2,v2), ... , (keyn,vn) ]
    def __vindex(self,loftuples, key):
        i=0
        for v in loftuples:
            if v[0]==key: return i
            i += 1
        return -1

    def __new_order(self,uid, oid, side, price, qty, exch):
        book = self.bookData.obooks[uid][side] # ref to symbol-side book

        if price not in book:
            book[price] = [] # create a new level
        if oid not in book[price]: # add new order if not exist
            book[price].append( (oid,qty,exch) ) # append order to price level list
        else:
            self.__modify_order(uid, oid, side, price, qty, exch)

    def __modify_order(self,uid, oid, side, price, delta_qty, exch):
        book = self.bookData.obooks[uid][side]

        if price not in book: # check if price exists first
            return
        else:
            indx = self.__vindex(book[price], oid) # get index of orderid(oid) in price level list
            if indx>=0: # if oid is valid
                x=book[price][indx]
                if delta_qty > 0:  # move to the back if quantity increases
                    del book[price][indx] # remove order first
                    # put it last in priority
                    book[price].append( (x[0], x[1]+delta_qty, exch))
                else:     # qty decrease => priority doesn't change
                    book[price][indx] = (x[0], x[1]+delta_qty, x[2])

    def __delete_order(self,uid, oid, side, price):
        book = self.bookData.obooks[uid][side]
        indx=-1
        if price not in book and price != 0: # wrong price information, ignore order
            return
        if price != 0: # We can use price info
            indx = self.__vindex(book[price], oid) # find index directly
        else: # EOBI - find price of particular orderID
            for price in book:
                indx = self.__vindex(book[price],oid)
                if indx != -1: break # break if correc price is found
        if indx>=0: # finally remove the order and the level if it's empty
            del book[price][indx]
            if len(book[price])==0:
                del book[price]
            
    
