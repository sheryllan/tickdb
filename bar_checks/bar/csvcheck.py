import datetime as dt

import bar.frontmonthtask as fmtask
from bar.csvcheck_config import *


fmtask.set_dbconfig(SERVERNAME)


class CsvCheckTask(fmtask.FrontMonthCheckTask):
    def __init__(self):
        super().__init__()

        self.aparser.add_argument('--source', nargs='*', type=str,
                                  help='the source directory of the data')
        self.aparser.add_argument('--email', action='store_true',
                                  help='set it to send email(s) of the report(s)')
        self.aparser.add_argument('--login', nargs='*', type=str, default=LOGIN,
                                  help='the login details of the sender, including username and password')
        self.aparser.add_argument('--recipients', nargs='*', type=str, default=RECIPIENTS,
                                  help='the email address of recipients')

    @property
    def task_source(self):
        return self.task_args.source

    @property
    def task_email(self):
        return self.task_args.email

    @property
    def task_login(self):
        return tuple(self.task_args.login)

    @property
    def task_recipients(self):
        return self.task_args.recipients

    def format_reports(self, barxml=None, tsxml=None, bar_title=BAR_TITLE, ts_title=TS_TITLE, **kwargs):
        self.set_taskargs(parse_args=True, **kwargs)
        barhtml = etree_tostr(to_styled_xml(barxml, BARXSL), self.task_barhtml) if barxml is not None else None
        tshtml = etree_tostr(to_styled_xml(tsxml, TSXSL), self.task_tshtml) if tsxml is not None else None

        if self.task_email:
            with EmailSession(*self.task_login) as session:
                if barhtml is not None:
                    session.email(self.task_recipients, barhtml, bar_title, self.split_barhtml)
                if tshtml is not None:
                    session.email(self.task_recipients, tshtml, ts_title, self.split_tshtml)



if __name__ == '__main__':
    task = FrontMonthCheckTask()
    logging.basicConfig(level=logging.INFO)

    # products = ['ES', 'NQ', 'YM', 'ZN', 'ZB', 'UB', '6E', '6J', '6B', 'GC', 'CL']
    products = ['ES', 'ZN']
    # task.run_checks(source=en.Server.FileConfig.REACTOR_GZIPS, product=products,
    #                 ptype='F', dtfrom=dt.date(2018, 12, 1), dtto=dt.date(2018, 12, 31),
    #                 schedule='CMESchedule', email=True)
    outfile_fmt = '2018-{}-{}'
    bar_files, ts_files = [], []
    for prod in products:
        f1, f2 = task.run_checks(product=prod,
                                 ptype='F', dtfrom=dt.date(2018, 1, 1), dtto=dt.date(2018, 12, 31),
                                 schedule='CMESchedule')
        task.set_taskargs(parse_args=True,  dtfrom=dt.date(2018, 1, 1), dtto=dt.date(2018, 12, 31),
                          barhtml=outfile_fmt.format(prod, 'bar_check.html'),
                          tshtml=outfile_fmt.format(prod, 'timeseries_check.html'))
        f1, f2 = task.task_barhtml, task.task_tshtml
        bar_files.append(f1)
        ts_files.append(f2)

    with EmailSession(*task.task_login) as email_session:
        email_session.email(task.task_recipients, bar_files, BAR_TITLE, task.split_barhtml)
        email_session.email(task.task_recipients, ts_files, TS_TITLE, task.split_tshtml)
