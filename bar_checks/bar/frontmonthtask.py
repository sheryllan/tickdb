import logging
from bar.taskconfig import *
import bar.enrichedOHLCVN as en
from htmlprocessor import *
from itertools import groupby
from influxcommon import limit, order_by


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


    def get_continuous_contracts(self, time_from, product=None, ptype=None, time_to=None):
        contracts = self.get_data(Continous.name(), time_from, product, ptype)
        start, end = 'start', 'end'
        contracts[start] = contracts.index
        sr_end = contracts[start][1:].reset_index(drop=True)
        sr_end.name = end

        results = pd.concat([contracts.reset_index(drop=True), sr_end], axis=1)
        if (not results.empty) and results.iloc[0][start] > time_from:
            head = self.get_data(Continous.name(), None, product, ptype, time_from, filters=[order_by('DESC'), limit(1)])
            if not head.empty:
                yield (time_from, head.index[0]), head.iloc[0]

        for i, row in results.iterrows():
            if pd.isna(row[end]):
                row[end] = None

            if row[end] > time_to >= row[start]:
                yield (row[start], time_to), row
                break
            yield (row[start], row[end]), row


    def split_email(self, html):

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

        for split in split_html(html, lambda x: x.find_all(TBODY), lambda x: find_all_by_depth(x, TR), self.ATTACHMENT_LIMIT,
                                lambda x, y: split_tags(x, y, grouping), True):

            yield split


    def run_checks(self, **kwargs):
        self.set_taskargs(**kwargs)
        contracts = self.get_continuous_contracts(self.task_dtfrom, self.task_product, self.task_ptype, self.task_dtto)

        data = pd.concat((self.get_data(en.Enriched.name(), time_start,
                                        row[TagsC.PRODUCT], row[TagsC.TYPE], time_end, row[TagsC.EXPIRY],
                                        include_from=True, include_to=False)
                          for (time_start, time_end), row in contracts))

        barhtml, tshtml = self.check(data)

        if self.task_email:
            with EmailSession(*self.task_login, self.split_email) as session:
                session.email(self.task_recipients, [barhtml, tshtml], [BAR_TITILE, TS_TITLE])



if __name__ == '__main__':
    task = FrontMonthCheckTask()

    logging.basicConfig(level=logging.INFO)
    # products = ['ZF', 'ZN', 'TN', 'ZB', 'UB', 'ES', 'NQ', 'YM', 'EMD', 'RTY', '6A', '6B', '6C', '6E', '6J', '6M', '6N',
    #             '6S', 'BTC', 'GC', 'SI', 'HG', 'CL', 'HO', 'RB']
    products = ['ES']
    task.run_checks(product=products, ptype='F', dtfrom=dt.date(2018, 8, 1), schedule='CMESchedule')
    # task.run_checks(schedule='CMESchedule')
    # task.email([task.task_barhtml, task.task_tshtml], [BAR_TITILE, TS_TITLE])