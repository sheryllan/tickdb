import logging
from bar.enrichedOHLCVN import TaskArguments

import bar.enrichedOHLCVN as en
from bar.bardataaccess import BarAccessor

from xmlconverter import *


def set_dbconfig(server):
    global Continous, TagsC, FieldsC
    en.set_dbconfig(server)
    Continous = en.Server.TABLES[en.Server.ContinuousContract.name()]
    TagsC = Continous.Tags
    FieldsC = Continous.Fields


class FrontMonthCheckTask(en.CheckTask):

    def __init__(self, task_args=TaskArguments()):
        super().__init__(BarAccessor.factory(en.Server.HOSTNAME), task_args)
        self.to_continuous_mapping = {self.args.PRODUCT: TagsC.PRODUCT,
                                      self.args.TYPE: TagsC.TYPE,
                                      self.args.EXPIRY: FieldsC.EXPIRY,
                                      self.args.DTFROM: self.accessor.TIME_FROM,
                                      self.args.DTTO: self.accessor.TIME_TO}

        self.to_enriched_mapping.update({self.to_continuous_mapping[k]: self.to_enriched_mapping[k]
                                         for k in self.to_continuous_mapping if k in self.to_enriched_mapping})

    def get_continuous_contracts(self, **kwargs):
        self.set_taskargs(**kwargs)
        new_kwargs = {self.to_continuous_mapping.get(k, k): v for k, v in self.args.arg_dict.items()}
        new_kwargs.update({self.to_continuous_mapping.get(k, k): v for k, v in kwargs.items()
                           if self.to_continuous_mapping.get(k, k) not in new_kwargs})
        return self.accessor.get_continuous_contracts(**new_kwargs)

    def run_check_task(self, **kwargs):
        contracts = self.get_continuous_contracts(**kwargs)

        barxml, tsxml = self.task_bar_etree, self.task_ts_etree
        fields = [TagsC.PRODUCT, TagsC.TYPE, FieldsC.EXPIRY]
        checked_products = set()
        for (time_start, time_end), row in contracts:
            contract_info = ','.join('{}: {}'.format(k, v) for k, v in row[fields].items())
            logging.info('Start: {}, End: {}, {}'.format(time_start, time_end, contract_info))
            time_args = {self.accessor.TIME_FROM: time_start,
                         self.accessor.TIME_TO: time_end,
                         self.accessor.INCLUDE_TO: time_end == self.args.dtto}
            data = self.get_bar_data(**row[fields].to_dict(), **time_args)

            if data is not None:
                self.set_taskargs(dtfrom=time_start, dtto=time_end)
                self.get_check_xmls(data, barxml, tsxml)
                checked_products.add(row[TagsC.PRODUCT])

        missing_products = [{self.PRODUCT: p} for p in to_iter(self.args.product) if p not in checked_products]
        if missing_products:
            barxml.insert(0, rcsv_addto_etree(missing_products, self.MISSING_PRODS))

        return barxml, tsxml
