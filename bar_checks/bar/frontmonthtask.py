import logging
from itertools import groupby

from bar.frontmonthtask_config import *
import bar.enrichedOHLCVN as en
from htmlprocessor import *
from dataaccess import *
from xmlconverter import *


en.set_dbconfig('lcmquantldn1')
Continous = en.Server.TABLES[en.Server.CONTINUOUS_CONTRACT]
TagsC = Continous.Tags
FieldsC = Continous.Fields


class FrontMonthCheckTask(en.CheckTask):
    ATTACHMENT_LIMIT = 20000000

    def __init__(self):
        super().__init__(FileAccessor(en.Server, 'slan', 'slanpass'))

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
        self.set_taskargs(parse_args=True, **kwargs)
        contracts = self.data_accessor.get_continuous_contracts(self.task_dtfrom, self.task_dtto,
                                                                **{TagsC.PRODUCT: self.task_product,
                                                                   TagsC.TYPE: self.task_ptype})

        barxml, tsxml = self.task_bar_etree, self.task_ts_etree
        fields = [TagsC.PRODUCT, TagsC.TYPE, TagsC.EXPIRY]

        for (time_start, time_end), row in contracts:
            data_args = {en.Server.TABLE: en.Enriched.name(), **row[fields].to_dict()}
            data = self.data_accessor.get_data(time_start, time_end,
                                               include_to=time_end == self.task_dtto,
                                               concat=False, **data_args)

            if data is not None:
                self.set_taskargs(dtfrom=time_start, dtto=time_end)
                barxml = self.bar_check_xml(data, barxml)
                tsxml = self.timeseries_check_xml(data, tsxml)

        etree_tostr(barxml, self.task_barxml)
        etree_tostr(tsxml, self.task_tsxml)
        #
        # missing_products = self.missing_products()
        # if missing_products is not None:
        #     barxml.insert(0, missing_products)
        #
        # barhtml = etree_tostr(to_styled_xml(barxml, BARXSL), self.task_barhtml)
        # tshtml = etree_tostr(to_styled_xml(tsxml, TSXSL), self.task_tshtml)
        #
        # if self.task_email:
        #     with EmailSession(*self.task_login) as session:
        #         session.email(self.task_recipients, barhtml, BAR_TITILE, self.split_barhtml)
        #         session.email(self.task_recipients, tshtml, TS_TITLE, self.split_tshtml)


if __name__ == '__main__':
    task = FrontMonthCheckTask()
    logging.basicConfig(level=logging.INFO)

    products = ['ES', 'NQ', 'YM', 'ZN', 'ZB', 'UB', '6E', '6J', '6B', 'GC', 'CL']
    task.run_checks(product=products,
                    ptype='F', dtfrom=dt.date(2018, 11, 22), dtto=dt.date(2018, 11, 24),
                    schedule='CMESchedule', barxml='bar.xml', tsxml='ts.xml')

    # task.run_checks(schedule='CMESchedule')
    # task.email([task.task_barhtml, task.task_tshtml], [BAR_TITILE, TS_TITLE])
