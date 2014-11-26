from BookData import *

class ETI:
    def __init__(self,date_string,prev_bookData=None):
        self.bookData=None
        if prev_bookData is not None:
            self.bookData=prev_bookData
        else:
            self.bookData=MessageSequenceTracker()    

    ############################################################
    #
    #   Parse Functions
    #
    ############################################################
    def __parse_tick(self,tick):
        uid=tick.values['secid']
        msgseqnum=int(tick.values['msgseqnum'])
        #exch= int(tick.values["trdregtstimein"])
        #recv= int(tick.timestamp)
        #side= int(tick.values["side"])-1
        #oid = int(tick.values["trdregTStimepriority"])
        #price=float(tick.values["price"])/100000000.0
        #qty = int(tick.values["qty"])
        #new_order(uid,oid,side,price,qty,exch)
        #printBook("A",uid,exch,recv)    
        #printTrade(uid,exch,recv,price,qty)    
        if tick.name=='eti_10100_new_ord':
            pass
        elif tick.name=='eti_10101_new_ord_resp':
            pass
        elif tick.name=='eti_10102_new_ord_s_resp':
            pass
        elif tick.name=='eti_10103_imm_exec_resp':
            pass
        elif tick.name=='eti_10104_book_order_exec':
            pass
        elif tick.name=='eti_10106_rep_ord':
            pass
        elif tick.name=='eti_10107_can_ord_resp':
            pass
        elif tick.name=='eti_10107_rep_ord_resp':
            pass
        elif tick.name=='eti_10108_rep_ord_resp_s':
            pass
        elif tick.name=='eti_10109_can_ord':
            pass
        elif tick.name=='eti_10111_can_ord_resp_s':
            pass
        elif tick.name=='eti_10112_can_ord_not':
            pass
        elif tick.name=='eti_10113_new_ord_m':
            pass
        elif tick.name=='eti_10114_rep_ord_m':
            pass
        elif tick.name=='eti_10120_mass_can_ord':
            pass
        elif tick.name=='eti_10121_10124_mass_can_resp':
            pass
        elif tick.name=='eti_10122_mass_can_not':
            pass
        elif tick.name=='eti_10123_can_ord_m':
            pass
        elif tick.name=='eti_10125_new_ord_s':
            pass
        elif tick.name=='eti_10126_rep_ord_s':
            pass
        elif tick.name=='eti_10401_10118_quote_cross_req':
            pass
        elif tick.name=='eti_10402_10119_quote_cross_req_resp':
            pass
        elif tick.name=='eti_10403_quote_activ_req':
            pass
        elif tick.name=='eti_10404_quote_activ_resp':
            pass
        elif tick.name=='eti_10405_mass_quote':
            pass
        elif tick.name=='eti_10406_mass_quote_resp':
            pass
        elif tick.name=='eti_10407_quote_exec':
            pass
        elif tick.name=='eti_10408_quote_mass_cancel_req':
            pass
        elif tick.name=='eti_10409_quote_mass_canc_resp':
            pass
        elif tick.name=='eti_10410_quote_mass_canc_not':
            pass
        elif tick.name=='eti_10411_quote_activ_not':
                pass

    ############################################################
    #
    #   Main Function
    #
    ############################################################    
    def calcETI(self,ticks):  #ticks is a list of DataMole ticks
        for tick in ticks:
            if tick.name=='eti-msg':
                continue
            # go through stored data first
            for suid in self.bookData.product_stored_data.keys():
                count=len(self.bookData.product_stored_data[suid])
                while count>0:
                    st_tick=self.bookData.product_stored_data[suid].pop(0)
                    if suid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[suid]>=msgseqnum:
                        pass  #this is the A/B feed case
                    elif suid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[suid]+1==msgseqnum:
                        self.bookData.product_sequence_numbers[suid]+=1
                        parse_tick(st_tick,date_string)
                    else:
                        self.bookData.product_stored_data[suid].append(st_tick)
                    count-=1
                if len(self.bookData.product_stored_data[suid])==0:
                    self.bookData.product_stored_data.pop(suid)
            # And now look at last tick
            uid=tick.values['secid']
            msgseqnum=int(tick.values['msgseqnum'])
            if uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]>=msgseqnum:
                continue  #this is the A/B feed case
            elif uid in self.bookData.product_sequence_numbers and self.bookData.product_sequence_numbers[uid]+1==msgseqnum:
                self.bookData.product_sequence_numbers[uid]+=1
                parse_tick(tick,date_string)
            else:
                if uid not in self.bookData.product_stored_data:
                    self.bookData.product_stored_data[uid]=[]
                self.bookData.product_stored_data[uid].append(tick)
    
