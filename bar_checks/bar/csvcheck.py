import datetime as dt
import logging

import pytz

import bar.frontmonthtask as fmtask
from bar.csvcheck_config import *
from bar.enrichedOHLCVN import TaskArguments
from timeutils.commonfuncs import last_n_days
from xmlconverter import *
from htmlprocessor import EmailSession


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

    SOURCE_MAP = {'qtg': 'gzips',
                  'reactor': 'reactor_gzips'}

    REPORT_NAME_SEP = '.'
    REPORT_TIME_FMT = {
        Report.ANNUAL.value: lambda *args: f'{args[0].year}',
        Report.DAILY.value: lambda *args: args[0].strftime('%Y%m%d'),
        Report.DATES.value: lambda *args: f'{args[0].strftime("%Y%m%d")}-{args[1].strftime("%Y%m%d")}'}

    REPORT_SOURCE_MAP = {SOURCE_MAP['qtg']: 'QTG',
                         SOURCE_MAP['reactor']: 'Reactor'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_defaults(**{self.WINDOW: WINDOW,
                             self.WINDOW_TZ: WINDOW_TZ,
                             self.SCHEDULE: SCHEDULE,
                             self.TIMEZONE: TIMEZONE})

        self.add_argument('--' + self.SOURCE, nargs='*', type=str, default=SOURCE,
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
        source = [self.SOURCE_MAP.get(v, v) for v in to_iter(value)]
        return source[0] if len(source) == 1 else source

    def _report_config(self, value):
        report_config = to_iter(value, ittype=tuple)
        rtype, rtime = str(report_config[0]), report_config[1:]

        if rtype == Report.ANNUAL.value:
            if not rtime:
                year = dt.datetime.now(pytz.timezone(self.TIMEZONE)).year - 1
            elif isinstance(rtime[0], dt.datetime):
                year = rtime[0].year
            elif str(rtime[0]).isdigit():
                year = int(rtime[0])
            else:
                raise ValueError('Invalid time setting for report_config: must be a datetime or positive integer')
            time_config = self._dtfrom((year, 1, 1)), self._dtto((year, 12, 31))

        elif rtype == Report.DAILY.value:
            if not rtime:
                dtfrom = last_n_days(1)
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
        self.check_out_xmls = {self.BAR_CHECK: lambda: self.args.barxml,
                               self.TIMESERIESE_CHECK: lambda: self.args.tsxml}
        self.check_out_htmls = {self.BAR_CHECK: lambda: self.args.barhtml,
                                self.TIMESERIESE_CHECK: lambda: self.args.tshtml}
        self.email_titles = {self.BAR_CHECK: lambda *args: f'{BAR_TITLE} {" ".join(args)}',
                             self.TIMESERIESE_CHECK: lambda *args: f'{TS_TITLE} {" ".join(args)}'}

    @property
    def bar_etree(self):
        tree = super().bar_etree
        root = tree.getroot()
        root.set(self.SOURCE, ', '.join(to_iter(self.args.source)))
        return tree

    @property
    def ts_etree(self):
        tree = super().ts_etree
        root = tree.getroot()
        root.set(self.SOURCE, ', '.join(to_iter(self.args.source)))
        return tree

    def run_report_task(self, **kwargs):
        checks_to_run = {check: self.check_etrees[check]() for check in self.args.check}
        xmls = self.run_check_task(checks_to_run, **kwargs)
        htmls = {}
        for check, xml in xmls.items():
            html = to_styled_xml(xml)
            htmls[check] = html

            if self.args.xml:
                write_etree(xml, self.check_out_xmls[check]())
            if self.args.html:
                write_etree(html, self.check_out_htmls[check](), method='html')

        return htmls

    def email_reports(self, reports, *args):
        with EmailSession(*self.args.login) as session:
            for check, html in reports.items():
                title = self.email_titles[check](*args)
                split_funcs = self.check_split_funcs[check]
                session.email_html(self.args.recipients, html, title, split_funcs)

    def run(self, **kwargs):
        self.set_taskargs(True, **kwargs)
        rtype, rtime = self.args.report_config
        self.set_taskargs(**{self.args.DTFROM: rtime[0], self.args.DTTO: rtime[1]})

        for src in to_iter(self.args.source, ittype=iter):
            self.set_taskargs(**{self.args.SOURCE: src})
            if not self.args.consolidate:
                reports = defaultdict(list)
                for prod in self.args.product:
                    self.set_taskargs(**{self.args.PRODUCT: prod})
                    htmls = self.run_report_task()
                    for check, html in htmls.items():
                        reports[check].append(html)
            else:
                reports = self.run_report_task()

            if self.args.email:
                self.email_reports(reports, self.args.REPORT_TIME_FMT[rtype](*rtime), f'({src})')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fmtask.set_dbconfig(SERVERNAME)

    task = CsvCheckTask()
    task.run()
    # products = ['ES', 'NQ', 'YM', 'ZN', 'ZB', 'UB', '6E', '6J', '6B', 'GC', 'CL']
    # task.set_taskargs(True)
    # task.email_reports({task.BAR_CHECK: ['reports/html/QTG.2019.ES.bar_check.html',
    #                                      'reports/html/QTG.2019.ZN.bar_check.html']})
