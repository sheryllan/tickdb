import logging
from pandas import DataFrame

import bar.enrichedOHLCVN as enriched
from bar.enrichedOHLCVN import TaskArguments


def set_dbconfig(server):
    global DataAccessor
    global Server, Barid
    global Continous, TagsC, FieldsC

    enriched.set_dbconfig(server)
    DataAccessor = enriched.DataAccessor
    Server = enriched.Server
    Barid = enriched.Barid
    Continous = Server.TABLES[Server.ContinuousContract.name()]
    TagsC = Continous.Tags
    FieldsC = Continous.Fields


class FrontMonthCheckTask(enriched.CheckTask):

    def __init__(self, data_accessor, taskargs_cls=TaskArguments):
        super().__init__(data_accessor, taskargs_cls)
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

    def run_check_task(self, check_xmls, **kwargs):
        contracts = self.get_continuous_contracts(**kwargs)
        fields = [TagsC.PRODUCT, TagsC.TYPE, FieldsC.EXPIRY]
        xmls = check_xmls
        dtto = self.args.dtto
        for (time_start, time_end), row in contracts:
            try:
                field_dict = row[fields].to_dict()
                contract_info = ','.join('{}: {}'.format(k, v) for k, v in field_dict.items())
                logging.info('Start: {}, End: {}, {}'.format(time_start, time_end, contract_info))

                empty_data = [({self.map_to_enriched.get(k, k): v for k, v in field_dict.items()}, DataFrame())]
                data_params = {**kwargs, **field_dict,
                               self.args.DTFROM: time_start,
                               self.args.DTTO: time_end,
                               self.accessor.INCLUDE_TO: time_end == dtto}
                data = self.get_bar_data(**data_params, empty=empty_data)
                xmls = self.check_integrated(data, check_xmls)
            except Exception as e:
                logging.error(e, exc_info=True)
        return xmls
