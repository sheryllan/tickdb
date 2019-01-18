import logging
import pytz
import datetime as dt

from bar.enrichedOHLCVN import TaskArguments
import bar.frontmonthtask as fmtask
from bar.csvcheck_config import *
from xmlconverter import *
from timeutils.commonfuncs import last_n_days


class CsvTaskArguments(TaskArguments):
    BARXML = 'barxml'
    TSXML = 'tsxml'
    BARHTML = 'barhtml'
    TSHTML = 'tshtml'

    EMAIL = 'email'
    LOGIN = 'login'
    RECIPIENTS = 'recipients'
    REPORT_CONFIG = 'report_config'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults(**{self.WINDOW: WINDOW,
                             self.WINDOW_TZ: WINDOW_TZ,
                             self.SCHEDULE: SCHEDULE,
                             self.TIMEZONE: TIMEZONE})

        self.add_argument('--source', nargs='*', type=str,
                          help='the source directory of the data')

        self.add_argument('--' + self.BARXML, nargs='?', type=str,
                          help='the xml output path of bar check')
        self.add_argument('--' + self.TSXML, nargs='?', type=str,
                          help='the xml output path of timeseries check')
        self.add_argument('--' + self.BARHTML, nargs='?', type=str, default=BARHTML,
                          help='the html output path of bar check after xsl transformation')
        self.add_argument('--' + self.TSHTML, nargs='?', type=str, default=TSHTML,
                          help='the html output path of time series check after xsl transformation')

        self.add_argument('--' + self.EMAIL, action='store_true',
                          help='set it to send email(s) of the report(s)')
        self.add_argument('--' + self.LOGIN, nargs='*', type=str, default=LOGIN,
                          help='the login details of the sender, including username and password')
        self.add_argument('--' + self.RECIPIENTS, nargs='*', type=str, default=RECIPIENTS,
                          help='the email address of recipients')

        self.add_argument('--' + self.REPORT_CONFIG, nargs='*', type=str, default=REPORT_CONFIG,
                          help='the configuration of report to run, including the type and time setting')

    @property
    def report_config(self):
        report_config = to_iter(self._arg_dict.get(self.REPORT_CONFIG))
        if len(report_config) == 2 and isinstance(report_config[1], dict):
            return report_config

        if report_config[0] == 'annual':
            year = dt.datetime.now(pytz.timezone(self.TIMEZONE)).year - 1 if len(report_config) < 2 else report_config[
                1]
            time_config = {self.DTFROM: (year, 1, 1), self.DTTO: (year, 12, 31)}

        elif report_config[0] == 'daily':
            dtfrom = last_n_days(1 if len(report_config) < 2 else report_config[1])
            dtto = last_n_days(-1, dtfrom)
            time_config = {self.DTFROM: dtfrom, self.DTTO: dtto}
        else:
            time_config = {self.DTFROM: self.dtfrom, self.DTTO: self.dtto}
        return report_config[0], time_config


class CsvCheckTask(fmtask.FrontMonthCheckTask):
    def __init__(self):
        super().__init__(CsvTaskArguments())

    def run_report_task(self, **kwargs):
        barxml, tsxml = self.run_check_task(**kwargs)
        etree_tostr(barxml, self.args.barxml)
        etree_tostr(tsxml, self.args.tsxml)
        return etree_tostr(to_styled_xml(barxml), self.args.barhtml, method='html'), \
               etree_tostr(to_styled_xml(tsxml), self.args.tshtml, method='html')

    def run(self, **kwargs):
        self.set_taskargs(True)
        bar_reports, ts_reports = [], []

        report_type, report_time = self.args.report_config
        kwargs.update(**report_time)
        if report_type == 'annual':
            year = report_time.values[0].year
            for prod in self.args.product:
                bar_report = ANNUAL_REPORT_FMT.format(year, prod, BARHTML)
                ts_report = ANNUAL_REPORT_FMT.format(year, prod, TSHTML)
                kwargs.update({self.args.PRODUCT: prod,
                               self.args.BARHTML: bar_report,
                               self.args.TSHTML: ts_report})
                self.run_report_task(**kwargs)
                bar_reports.append(bar_report)
                ts_reports.append(ts_report)

        elif self.args.report_config[0] == 'daily':
            kwargs.update({self.args.BARHTML: DAILY_BAR_REPORT,
                           self.args.TSHTML: DAILY_TS_REPORT})
            self.run_report_task(**kwargs)
            bar_reports.append(self.args.barhtml)
            ts_reports.append(self.args.tshtml)

        else:
            self.run_report_task(**kwargs)
            bar_reports.append(self.args.barhtml)
            ts_reports.append(self.args.tshtml)

        if self.args.email:
            self.email_reports(self.args.login, self.args.recipients, bar_reports, ts_reports, BAR_TITLE, TS_TITLE)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fmtask.set_dbconfig(SERVERNAME)

    task = CsvCheckTask()
    task.run()
    # products = ['ES', 'NQ', 'YM', 'ZN', 'ZB', 'UB', '6E', '6J', '6B', 'GC', 'CL']
    # task.set_taskargs(True)
    # task.email_reports(task.task_login, task.task_recipients,
    #                    ['2018-ES-bar_check.html', '2018-ZN-bar_check.html'],
    #                    ['2018-ES-timeseries_check.html', '2018-ZN-timeseries_check.html'],
    #                    BAR_TITLE, TS_TITLE)
