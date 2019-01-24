import logging
import pytz
import datetime as dt

from bar.enrichedOHLCVN import TaskArguments
import bar.frontmonthtask as fmtask
from bar.csvcheck_config import *
from xmlconverter import *
from timeutils.commonfuncs import last_n_days


class CsvTaskArguments(TaskArguments):
    SOURCE = 'source'

    BARXML = 'barxml'
    TSXML = 'tsxml'
    BARHTML = 'barhtml'
    TSHTML = 'tshtml'

    EMAIL = 'email'
    LOGIN = 'login'
    RECIPIENTS = 'recipients'
    REPORT_CONFIG = 'report_config'

    REPORT_FMT = os.path.join(REPORT_DIR, '{}.{}')
    REPORT_TIME_FMT = {
        Report.ANNUAL.value: lambda *args: f'{args[0].year}.{args[-1]}',
        Report.DAILY.value: lambda *args: args[0].strftime('%Y%m%d'),
        Report.DATES.value: lambda *args: f'{args[0].strftime("%Y%m%d")}-{args[1].strftime("%Y%m%d")}'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults(**{self.WINDOW: WINDOW,
                             self.WINDOW_TZ: WINDOW_TZ,
                             self.SCHEDULE: SCHEDULE,
                             self.TIMEZONE: TIMEZONE})

        self.add_argument('--' + self.SOURCE, nargs='*', type=str,
                          help='the source directory of the data')

        self.add_argument('--' + self.BARXML, nargs='?', type=str,
                          help='the xml output path of bar check')
        self.add_argument('--' + self.TSXML, nargs='?', type=str,
                          help='the xml output path of timeseries check')

        self.add_argument('--' + self.EMAIL, action='store_true',
                          help='set it to send email(s) of the report(s)')
        self.add_argument('--' + self.LOGIN, nargs='*', type=str, default=LOGIN,
                          help='the login details of the sender, including username and password')
        self.add_argument('--' + self.RECIPIENTS, nargs='*', type=str, default=RECIPIENTS,
                          help='the email address of recipients')

        self.add_argument('--' + self.REPORT_CONFIG, nargs='*', type=str, default=Report.DATES.value,
                          help='the configuration of report to run, including the type and time setting')

    def report_path(self, extension):
        rtype, rtime = self.report_config
        return self.REPORT_FMT.format(self.REPORT_TIME_FMT[rtype](*rtime, str(self.product)), extension)

    @property
    def barhtml(self):
        return self.report_path(BARHTML)

    @property
    def tshtml(self):
        return self.report_path(TSHTML)

    def _report_config(self, value):
        report_config = to_iter(value, ittype=tuple)
        rtype, rtime = (str(report_config[0]), None) if len(report_config) == 1 \
            else (str(report_config[0]), report_config[1:])

        if rtype == Report.ANNUAL.value:
            if rtime is None:
                year = dt.datetime.now(pytz.timezone(self.TIMEZONE)).year - 1
            elif isinstance(rtime[0], dt.datetime):
                year = rtime[0].year
            elif str(rtime[0]).isdigit():
                year = int(rtime[0])
            else:
                raise ValueError('Invalid time setting for report_config: must be a datetime or positive integer')
            time_config = (dt.date(year, 1, 1), dt.date(year, 12, 31))

        elif rtype == Report.DAILY.value:
            if rtime is None:
                dtfrom = last_n_days(2)
            elif isinstance(rtime[0], dt.datetime):
                dtfrom = rtime[0]
            elif str(rtime[0]).isdigit():
                dtfrom = last_n_days(int(rtime[0]))
            else:
                raise ValueError('Invalid time setting for report_config: must be a datetime or positive integer')
            dtto = last_n_days(-1, dtfrom)
            time_config = (dtfrom, dtto)

        elif rtype == Report.DATES.value:
            time_config = (self.dtfrom, self.dtto)

        else:
            raise ValueError('Invalid report type to set')

        return rtype, time_config


class CsvCheckTask(fmtask.FrontMonthCheckTask):
    SOURCE = 'source'

    def __init__(self):
        super().__init__(CsvTaskArguments())

    @property
    def task_bar_etree(self):
        tree = super().task_bar_etree
        root = tree.getroot()
        root.set(self.SOURCE, ', '.join(to_iter(self.args.source)))
        return tree

    @property
    def task_ts_etree(self):
        tree = super().task_ts_etree
        root = tree.getroot()
        root.set(self.SOURCE, ', '.join(to_iter(self.args.source)))
        return tree

    def run_report_task(self, barxml=None, tsxml=None, write_xml=True, **kwargs):
        barxml, tsxml = self.run_check_task(barxml, tsxml, **kwargs)
        if write_xml:
            write_etree(barxml, self.args.barxml)
            write_etree(tsxml, self.args.tsxml)
        barhtml, tshtml = to_styled_xml(barxml), to_styled_xml(tsxml)
        write_etree(barhtml, self.args.barhtml, method='html')
        write_etree(tshtml, self.args.tshtml, method='html')
        return barhtml, tshtml

    def run(self, **kwargs):
        self.set_taskargs(True, **kwargs)
        bar_reports, ts_reports = [], []
        rtype, rtime = self.args.report_config
        self.set_taskargs(**{self.args.DTFROM: rtime[0], self.args.DTTO: rtime[1]})
        if rtype == Report.ANNUAL.value:
            barxml, tsxml = self.task_bar_etree, self.task_ts_etree
            for prod in self.args.product:
                self.set_taskargs(**{self.args.PRODUCT: prod})
                barhtml, tshtml = self.run_report_task(barxml.getroot(), tsxml.getroot(), False)
                bar_reports.append(barhtml)
                ts_reports.append(tshtml)
            write_etree(barxml, self.args.barxml)
            write_etree(tsxml, self.args.tsxml)
        else:
            barhtml, tshtml = self.run_report_task()
            bar_reports.append(barhtml)
            ts_reports.append(tshtml)

        if self.args.email:
            title_time = self.args.REPORT_TIME_FMT[rtype](*rtime, '').split('.')[0]
            bar_title = ' '.join([BAR_TITLE, title_time, f'({self.args.source})'])
            ts_title = ' '.join([TS_TITLE, title_time, f'({self.args.source})'])
            self.email_reports(self.args.login, self.args.recipients, bar_reports, ts_reports, bar_title, ts_title)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fmtask.set_dbconfig(SERVERNAME)

    task = CsvCheckTask()
    task.run()
    # products = ['ES', 'NQ', 'YM', 'ZN', 'ZB', 'UB', '6E', '6J', '6B', 'GC', 'CL']
    # task.set_taskargs(True)
    # task.email_reports(task.args.login, task.args.recipients,
    #                    None, 'timeseries_check.html',
    #                    BAR_TITLE, TS_TITLE)
