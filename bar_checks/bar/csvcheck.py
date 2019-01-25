import datetime as dt
import logging

import pytz

import bar.frontmonthtask as fmtask
from bar.csvcheck_config import *
from bar.enrichedOHLCVN import TaskArguments
from timeutils.commonfuncs import last_n_days
from xmlconverter import *


class CsvTaskArguments(TaskArguments):
    SOURCE = 'source'

    OUTDIR = 'outdir'
    XML = 'xml'
    HTML = 'html'
    CONSOLIDATE = 'consolidate'

    BARXML = 'barxml'
    TSXML = 'tsxml'
    BARHTML = 'barhtml'
    TSHTML = 'tshtml'

    EMAIL = 'email'
    LOGIN = 'login'
    RECIPIENTS = 'recipients'
    REPORT_CONFIG = 'report_config'

    REPORT_NAME_SEP = '.'
    REPORT_TIME_FMT = {
        Report.ANNUAL.value: lambda *args: f'{args[0].year}',
        Report.DAILY.value: lambda *args: args[0].strftime('%Y%m%d'),
        Report.DATES.value: lambda *args: f'{args[0].strftime("%Y%m%d")}-{args[1].strftime("%Y%m%d")}'}
    REPORT_SOURCE_MAP = {'gzips': 'QTG', 'reactor_gzips': 'Reactor'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults(**{self.WINDOW: WINDOW,
                             self.WINDOW_TZ: WINDOW_TZ,
                             self.SCHEDULE: SCHEDULE,
                             self.TIMEZONE: TIMEZONE})

        self.add_argument('--' + self.SOURCE, nargs='*', type=str,
                          help='the source directory of the data')

        self.add_argument('--' + self.OUTDIR, nargs='?', type=str, default=DIR,
                          help='the output directory of the checks')
        self.add_argument('--' + self.XML, action='store_true',
                          help='set it to save the xml output to files')
        self.add_argument('--' + self.HTML, action='store_true',
                          help='set it to save the html output to files')
        self.add_argument('--' + self.CONSOLIDATE, action='store_true',
                          help='set it to consolidate the output to a single file')

        self.add_argument('--' + self.EMAIL, action='store_true',
                          help='set it to send email(s) of the report(s)')
        self.add_argument('--' + self.LOGIN, nargs='*', type=str, default=LOGIN,
                          help='the login details of the sender, including username and password')
        self.add_argument('--' + self.RECIPIENTS, nargs='*', type=str, default=RECIPIENTS,
                          help='the email address of recipients')

        self.add_argument('--' + self.REPORT_CONFIG, nargs='*', type=str, default=Report.DATES.value,
                          help='the configuration of report to run, including the type and time setting')

    def report_path(self, name, extension):
        path = os.path.join(self.outdir, REPORTS, extension)
        os.makedirs(path, exist_ok=True)

        rtype, rtime = self.report_config
        names = [self.REPORT_SOURCE_MAP.get(self.source, ''), self.REPORT_TIME_FMT[rtype](*rtime)] + \
                ([str(self.product)] if not self.consolidate else []) +\
                [name, extension]

        filename = self.REPORT_NAME_SEP.join(names)
        return os.path.join(path, filename)

    @property
    def barhtml(self):
        return self.report_path(BAR_REPORT_NAME, self.HTML)

    @property
    def tshtml(self):
        return self.report_path(TS_REPORT_NAME, self.HTML)

    @property
    def barxml(self):
        return self.report_path(BAR_REPORT_NAME, self.XML)

    @property
    def tsxml(self):
        return self.report_path(TS_REPORT_NAME, self.XML)

    def _source(self, value):
        source = to_iter(value)
        return source[0] if len(source) == 1 else source

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
            time_config = self._dtfrom((year, 1, 1)), self._dtto((year, 12, 31))

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
            time_config = self._dtfrom(dtfrom), self._dtto(dtto)

        elif rtype == Report.DATES.value:
            time_config = self.dtfrom, self.dtto

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

    def run_report_task(self, **kwargs):
        barxml, tsxml = self.task_bar_etree, self.task_ts_etree
        self.run_check_task(barxml, tsxml, **kwargs)
        if self.args.xml:
            write_etree(barxml, self.args.barxml)
            write_etree(tsxml, self.args.tsxml)
        barhtml, tshtml = to_styled_xml(barxml), to_styled_xml(tsxml)
        if self.args.html:
            write_etree(barhtml, self.args.barhtml, method='html')
            write_etree(tshtml, self.args.tshtml, method='html')
        return barhtml, tshtml

    def run(self, **kwargs):
        self.set_taskargs(True, **kwargs)
        rtype, rtime = self.args.report_config
        self.set_taskargs(**{self.args.DTFROM: rtime[0], self.args.DTTO: rtime[1]})

        for src in to_iter(self.args.source, ittype=iter):
            bar_reports, ts_reports = [], []
            self.set_taskargs(**{self.args.SOURCE: src})
            if not self.args.consolidate:
                for prod in self.args.product:
                    self.set_taskargs(**{self.args.PRODUCT: prod})
                    barhtml, tshtml = self.run_report_task()
                    bar_reports.append(barhtml)
                    ts_reports.append(tshtml)
            else:
                barhtml, tshtml = self.run_report_task()
                bar_reports.append(barhtml)
                ts_reports.append(tshtml)

            if self.args.email:
                title_time = self.args.REPORT_TIME_FMT[rtype](*rtime).split('.')
                bar_title = ' '.join([BAR_TITLE, title_time, f'({src})'])
                ts_title = ' '.join([TS_TITLE, title_time, f'({src})'])
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
