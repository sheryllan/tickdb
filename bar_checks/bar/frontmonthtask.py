import logging
from bar.checktask_config import *
from bar.frontmonthtask_config import *
import bar.enrichedOHLCVN as en
from htmlprocessor import *
from itertools import groupby
from influxcommon import limit, order_by
from xmlconverter import *


en.set_dbconfig('quantsim1')
Continous = en.Server.CONTINUOUS_CONTRACT
TagsC = Continous.Tags
FieldsC = Continous.Fields


class FrontMonthCheckTask(en.CheckTask):
    ATTACHMENT_LIMIT = 20000000

    def __init__(self):
        super().__init__()

        self.aparser.add_argument('--email', action='store_true',
                                  help='set it to send email(s) of the report(s)')
        self.aparser.add_argument('--login', nargs='*', type=str, default=LOGIN,
                                  help='the login details of the sender, including username and password')
        self.aparser.add_argument('--recipients', nargs='*', type=str, default=RECIPIENTS,
                                  help='the email address of recipients')

    @property
    def task_email(self):
        return self.task_args.email

    @property
    def task_login(self):
        return tuple(self.task_args.login)

    @property
    def task_recipients(self):
        return self.task_args.recipients


    def get_continuous_contracts(self, product=None, ptype=None, time_from=None, time_to=None):
        fields = {TagsC.PRODUCT: product, TagsC.TYPE: ptype}
        contracts = self.get_data(Continous.name(), time_from, time_to, empty=pd.DataFrame(), **fields)

        if contracts.empty or contracts.index[0] > time_from:
            others = [order_by('DESC'), limit(1)]
            prev_contract = self.get_data(Continous.name(), time_to=time_from, others=others, empty=pd.DataFrame(), **fields)
            prev_contract.index = pd.Index([time_from])
            contracts = prev_contract.append(contracts)

        start, end = 'start', 'end'
        contracts[start] = contracts.index
        sr_end = contracts[start][1:].append(pd.Series([time_to]), ignore_index=True)
        contracts[end] = sr_end.tolist()

        for i, row in contracts.iterrows():
            yield (row[start], row[end]), row


    def split_barhtml(self, html):

        def grouping(tags):

            def is_bar_row(tr):
                th = tr.find(TH, recursive=False)
                return th is not None and int(th[COLSPAN]) > 1

            bar = []
            for is_bar, group in groupby(tags, is_bar_row):
                if is_bar:
                    bar = list(group)
                else:
                    bar.extend(group)
                    yield bar
                    bar = []

        for split in split_html(html, lambda x: x.find_all(TBODY), lambda x: find_all_by_depth(x, TR),
                                self.ATTACHMENT_LIMIT, lambda x, y: split_tags(x, y, grouping)):

            yield split


    def split_tshtml(self, html):
        for split in split_html(html, lambda x: x.find_all(BODY), lambda x: find_all_by_depth(x, TABLE),
                                self.ATTACHMENT_LIMIT, split_tags):
            yield split


    def run_checks(self, **kwargs):
        self.set_taskargs(**kwargs)
        contracts = self.get_continuous_contracts(self.task_product, self.task_ptype, self.task_dtfrom, self.task_dtto)
        fields = [TagsC.PRODUCT, TagsC.TYPE, TagsC.EXPIRY]

        barxml, tsxml = self.task_bar_etree, self.task_ts_etree
        for (time_start, time_end), row in contracts:
            data = self.get_data(en.Enriched.name(), time_start, time_end, include_to=time_end == self.task_dtto,
                              **row[fields].to_dict())

            if data is not None:
                barxml = self.bar_checks_xml(data, barxml, self.task_barxml)
                tsxml = self.timeseries_checks_xml(data, tsxml, self.task_tsxml, start_date=time_start, end_date=time_end)

        missing_products = self.missing_products()
        if missing_products is not None:
            barxml.insert(0, missing_products)

        barhtml = etree_tostr(to_styled_xml(barxml, BARXSL), self.task_barhtml)
        tshtml = etree_tostr(to_styled_xml(tsxml, TSXSL), self.task_tshtml)

        if self.task_email:
            with EmailSession(*self.task_login) as session:
                session.email(self.task_recipients, barhtml, BAR_TITILE, self.split_barhtml)
                session.email(self.task_recipients, tshtml, TS_TITLE, self.split_tshtml)


if __name__ == '__main__':
    task = FrontMonthCheckTask()
    logging.basicConfig(level=logging.INFO)
    # products = ['ZF', 'ZN', 'TN', 'ZB', 'UB', 'ES', 'NQ', 'YM', 'EMD', 'RTY', '6A', '6B', '6C', '6E', '6J', '6M', '6N',
    #             '6S', 'BTC', 'GC', 'SI', 'HG', 'CL', 'HO', 'RB']
    task.run_checks(schedule='CMESchedule')
    # task.run_checks(product=products, ptype='F', dtfrom=dt.date(2018, 3, 1), dtto=dt.date(2018, 6, 1), schedule='CMESchedule')
    # task.email([task.task_barhtml, task.task_tshtml], [BAR_TITILE, TS_TITLE])