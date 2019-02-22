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

    def __init__(self, taskargs_cls=TaskArguments):
        super().__init__(BarAccessor.factory(en.Server.HOSTNAME), taskargs_cls)
        self.map_to_continuous = {self.args.PRODUCT: TagsC.PRODUCT,
                                  self.args.TYPE: TagsC.TYPE,
                                  self.args.EXPIRY: FieldsC.EXPIRY,
                                  self.args.DTFROM: self.accessor.TIME_FROM,
                                  self.args.DTTO: self.accessor.TIME_TO}

        self.map_to_enriched.update({self.map_to_continuous[k]: self.map_to_enriched[k]
                                     for k in self.map_to_continuous if k in self.map_to_enriched})

    def get_continuous_contracts(self, **kwargs):
        new_kwargs = {self.map_to_continuous.get(k, k): v for k, v in kwargs.items()}
        taskargs = {k: new_kwargs[self.map_to_continuous.get(k, k)] for k, v in self.args.arg_dict.items()
                    if self.map_to_continuous.get(k, k) in new_kwargs}
        self.set_taskargs(**taskargs)
        new_kwargs.update({self.map_to_continuous.get(k, k): v for k, v in self.args.arg_dict.items()})
        return self.accessor.get_continuous_contracts(**new_kwargs)

    def run_check_task(self, checks_to_run, **kwargs):
        contracts = self.get_continuous_contracts(**kwargs)
        fields = [TagsC.PRODUCT, TagsC.TYPE, FieldsC.EXPIRY]
        xmls = checks_to_run
        for (time_start, time_end), row in contracts:
            field_dict = row[fields].to_dict()
            contract_info = ','.join('{}: {}'.format(k, v) for k, v in field_dict.items())
            logging.info('Start: {}, End: {}, {}'.format(time_start, time_end, contract_info))

            empty_data = [({self.map_to_enriched.get(k, k): v for k, v in field_dict.items()}, DataFrame())]
            data = self.get_bar_data(**field_dict,
                                     **{self.args.DTFROM: time_start,
                                        self.args.DTTO: time_end,
                                        self.accessor.INCLUDE_TO: time_end == self.args.dtto},
                                     empty=empty_data)
            xmls = self.check_integrated(data, checks_to_run)
        return xmls
