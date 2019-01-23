import logging
from pandas import DataFrame

import bar.enrichedOHLCVN as en
from bar.bardataaccess import BarAccessor
from bar.enrichedOHLCVN import TaskArguments


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
        new_kwargs = {self.to_continuous_mapping.get(k, k): v for k, v in kwargs.items()}
        taskargs = {k: new_kwargs[self.to_continuous_mapping.get(k, k)] for k, v in self.args.arg_dict.items()
                    if self.to_continuous_mapping.get(k, k) in new_kwargs}
        self.set_taskargs(**taskargs)
        new_kwargs.update({self.to_continuous_mapping.get(k, k): v for k, v in self.args.arg_dict.items()})
        return self.accessor.get_continuous_contracts(**new_kwargs)

    def run_check_task(self, **kwargs):
        contracts = self.get_continuous_contracts(**kwargs)

        barxml, tsxml = self.task_bar_etree, self.task_ts_etree
        fields = [TagsC.PRODUCT, TagsC.TYPE, FieldsC.EXPIRY]
        for (time_start, time_end), row in contracts:
            field_dict = row[fields].to_dict()
            contract_info = ','.join('{}: {}'.format(k, v) for k, v in field_dict.items())
            logging.info('Start: {}, End: {}, {}'.format(time_start, time_end, contract_info))

            empty_data = [({self.to_enriched_mapping.get(k, k): v for k, v in field_dict.items()}, DataFrame())]
            data = self.get_bar_data(**field_dict,
                                     **{self.args.DTFROM: time_start,
                                        self.args.DTTO: time_end,
                                        self.accessor.INCLUDE_TO: time_end == self.args.dtto},
                                     empty=empty_data)
            self.check_integrated(data, barxml, tsxml)
        return barxml, tsxml
