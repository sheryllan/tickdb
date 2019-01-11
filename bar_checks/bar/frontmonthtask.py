import logging
from itertools import groupby

import bar.enrichedOHLCVN as en
from bar.bardataaccess import accessor_factory

from htmlprocessor import *
from xmlconverter import *


def set_dbconfig(server):
    global Continous, TagsC, FieldsC
    en.set_dbconfig(server)
    Continous = en.Server.TABLES[en.Server.ContinuousContract.name()]
    TagsC = Continous.Tags
    FieldsC = Continous.Fields


class FrontMonthCheckTask(en.CheckTask):
    def __init__(self):
        super().__init__(accessor_factory(en.Server.HOSTNAME))

    def split_barhtml(self, html, size_limit):
        def grouping(tr_tags):
            def is_bar_tr(tr):
                th = tr.find(TH, recursive=False)
                return th is not None and int(th[COLSPAN]) > 1

            bar = []
            for is_bar, tr_group in groupby(tr_tags, is_bar_tr):
                if is_bar:
                    bar = list(tr_group)
                else:
                    bar.extend(tr_group)
                    yield bar

        yield from split_html(
            html,
            lambda x: x.find_all(TBODY),
            lambda x: find_all_by_depth(x, TR),
            size_limit,
            lambda x, y: split_tags(x, y, grouping)
        )

    def split_tshtml(self, html, size_limit):
        yield from split_html(
                html,
                lambda x: x.find_all(BODY),
                lambda x: find_all_by_depth(x, TABLE),
                size_limit,
                split_tags
        )

    def run_checks(self, **kwargs):
        self.set_taskargs(parse_args=True, **kwargs)
        cc_kwargs = {TagsC.PRODUCT: self.task_product, TagsC.TYPE: self.task_ptype, Continous.SOURCE: self.task_source}
        contracts = self.accessor.get_continuous_contracts(self.task_dtfrom, self.task_dtto, **cc_kwargs)

        barxml, tsxml = self.task_bar_etree, self.task_ts_etree
        fields = [TagsC.PRODUCT, TagsC.TYPE, FieldsC.EXPIRY]
        checked_products = set()
        for (time_start, time_end), row in contracts:
            contract_info = ','.join('{}: {}'.format(k, v) for k, v in row[fields].items())
            logging.info('Start: {}, End: {}, {}'.format(time_start, time_end, contract_info))

            data_args = {
                self.accessor.TIME_FROM: time_start,
                self.accessor.TIME_TO: time_end,
                self.accessor.INCLUDE_TO: time_end == self.task_dtto,
                en.Enriched.SOURCE: self.task_source,
                en.Enriched.YEAR: [time_start.year, time_end.year],
                **row[fields].to_dict()}
            data = self.accessor.get_data(en.Enriched.name(), concat=False, **data_args)

            if data is not None:
                self.set_taskargs(dtfrom=time_start, dtto=time_end)
                barxml = self.bar_check_xml(data, barxml)
                tsxml = self.timeseries_check_xml(data, tsxml)
                checked_products.add(row[TagsC.PRODUCT])

        missing_products = [{self.PRODUCT: p} for p in to_iter(self.task_product) if p not in checked_products]
        if missing_products:
            barxml.insert(0, rcsv_addto_etree(missing_products, self.MISSING_PRODS))

        etree_tostr(barxml, self.task_barxml, header='xml')
        etree_tostr(tsxml, self.task_tsxml, header='xml')
        return barxml, tsxml


    def



