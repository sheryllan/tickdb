from BookData import *


class EOBI:
    def __init__(self,levels,date_string,prev_bookData=None):
        self.bookData=None
        if levels<1:
            raise Exception("Levels must be 1 or more")
        if prev_bookData is not None:
            self.bookData=prev_bookData
        else:
            self.bookData=ByOrderBookData(levels,date_string)        
        
    def calcEOBI(self,ticks):  #ticks is a list of DataMole ticks
        ## TODO... need to handle getting the snapshot and then handling incremental
        for tick in ticks: # read file line by line
            if tick.name=='eobi_header':
                continue
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
            uid=tick.values['secid']
            msgseqnum=int(tick.values['msgseqnum'])
            if tick.name=='eobi_13100_ord_add' and uid not in self.bookData.obooks:#TODO remove this. as it is taking the place of snapshoting.
                self.bookData.init_book(uid,msgseqnum-1)
            if uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]>=msgseqnum:
                continue  #this is the A/B feed case
            elif uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]+1==msgseqnum:
                self.bookData.product_sequence_numbers[uid]+=1
                self.__parse_tick(tick)
            else:
                if uid not in self.bookData.product_stored_data:
                    self.bookData.product_stored_data[uid]=[]
                self.bookData.product_stored_data[uid].append(tick)
        #TODO:  We need to check that we had a clean parse... once we are completely done we need to check what is in self.bookData.product_sequence_numbers
        return self.bookData
        
    ############################################################
    #
    #   Parse Functions
    #
    ############################################################        
    def __parse_tick(self,tick):
        uid=tick.values['secid']
        msgseqnum=int(tick.values['msgseqnum'])
        #we ignore  'eobi_13504_top_of_book'    
        if tick.name=='SUBSCRIBE':
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
            exch= int(tick.values["trdregtstimein"])
            recv= int(tick.timestamp)
            side= int(tick.values["side"])-1
            oid = int(tick.values["trdregTStimepriority"])
            price=float(tick.values["price"])/100000000.0
            
            if tick.name=='eobi_13100_ord_add':
                qty = int(tick.values["qty"])
                self.__new_order(uid,oid,side,price,qty,exch)
                self.bookData.printBook("A",uid,exch,recv)
                
            elif tick.name=='eobi_13101_order_mod':
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
                self.__delete_order(uid, oid, side, 0)
                self.bookData.printBook("D",uid, exch,recv)    
                
            elif tick.name=='eobi_13104_13105_order_exec':
                qty = int(tick.values["qty"])
                self.bookData.printTrade(uid,exch,recv,price,qty)
            
            elif tick.name=='eobi_13106_order_mod_same_priority':
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
            
    
