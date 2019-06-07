import datetime as dt
import logging
from numpy import cumsum

import bar.enrichedOHLCVN as enriched
import bar.frontmonthtask as fmtask
from bar.csvcheck_config import *
from bar.enrichedOHLCVN import TaskArguments
from htmlprocessor import *
from timeutils.commonfuncs import last_n_days


def set_dbconfig(server):
    global DataAccessor
    global Server, Barid
    global Enriched, Fields, Tags

    fmtask.set_dbconfig(server)
    DataAccessor = fmtask.DataAccessor
    Server = fmtask.Server
    Barid = fmtask.Barid
    Enriched = Server.TABLES[Server.EnrichedOHLCVN.name()]
    Fields, Tags = Enriched.Fields, Enriched.Tags


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
        Report.ANNUAL: lambda *args: f'{args[0].year}',
        Report.DAILY: lambda *args: args[0].strftime('%Y%m%d'),
        Report.DATES: lambda *args: f'{args[0].strftime("%Y%m%d")}-{args[1].strftime("%Y%m%d")}'}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.source_names = Server.FileConfig.SourceNames
        self.source_report_map = {k: v.name for k, v in self.source_names._value2member_map_.items()}

        self.set_defaults(**{self.TIMEZONE: TIMEZONE})

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

        self.add_argument('--' + self.REPORT_CONFIG, nargs='*', type=str, default=Report.DATES,
                          help='the configuration of report to run, including the type and time setting')

    def report_path(self, name, extension):
        path = os.path.join(self.outdir, REPORTS, extension)
        os.makedirs(path, exist_ok=True)

        rtype, rtime = self.report_config
        product_name = str(self.product) if not self.consolidate else ''
        names = ['-'.join(self.source_report_map[s] for s in self.source),
                 self.REPORT_TIME_FMT[rtype](*rtime),
                 product_name, name, extension]
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
        if value is None:
            return self.get_default(self.SOURCE)
        source = to_iter(value, dtype=str)
        members = self.source_names.__members__
        for i, s in enumerate(source):
            s = members.get(s, s)
            if s not in self.source_report_map:
                raise ValueError('Invalid input of argument [source]: {}'.format(value))
            source[i] = s
        return source

    def _report_config(self, value):
        report_config = to_iter(value, ittype=tuple)
        rtype, rtime = str(report_config[0]), report_config[1:]

        if rtype == Report.ANNUAL:
            if not rtime:
                year = dt.datetime.now(self.timezone).year
            elif isinstance(rtime[0], dt.datetime):
                year = rtime[0].year
            elif str(rtime[0]).isdigit():
                year = int(rtime[0])
            else:
                raise ValueError('Invalid time setting for report_config: must be a datetime or positive integer')
            time_config = self._dtfrom((year, 1, 1)), self._dtto((year, 12, 31))

        elif rtype == Report.DAILY:
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

        elif rtype == Report.DATES:
            time_config = self.dtfrom, self.dtto

        else:
            raise ValueError('Invalid report type to set')

        return rtype, time_config


class SubCheckTask(enriched.SubCheckTask):
    SOURCE = 'source'
    TITLE = 'Check Report'

    @property
    def xml_etree(self):
        tree = super().xml_etree
        root = tree.getroot()
        root.set(self.SOURCE, ', '.join(self.args.source))
        return tree

    def html_status(self, html):
        root = to_elementtree(html).getroot()
        return int(root.get('status'))

    def email_title(self, status, *args):
        return f'{status}: {self.TITLE} {" ".join(args)}'

    def bar_series_check_xml(self, data, bar, root=None):
        raise NotImplementedError


class BarCheckTask(enriched.BarCheckTask, SubCheckTask):
    TITLE = BAR_TITLE

    @property
    def xml_file(self):
        return self.args.barxml

    @property
    def html_file(self):
        return self.args.barhtml

    def split_html(self, html, size_limit):
        def grouping(trs):
            th_xpath = XPathBuilder.find_expr(tag=TH)
            by = cumsum([True if tr.find(th_xpath) is not None else False for tr in trs])
            for _, tr_group in groupby(zip(trs, by), lambda x: x[1]):
                yield list(map(lambda x: x[0], tr_group))

        tbody_xpath = XPathBuilder.find_expr(relation=XPathBuilder.DESCENDANT, tag=TBODY)
        tr_xpath = XPathBuilder.find_expr(relation=XPathBuilder.DESCENDANT, tag=TR)
        yield from split_from_element(html, tbody_xpath, tr_xpath, size_limit, grouping)


class SeriesChecker(enriched.SeriesChecker):

    @classmethod
    def invalid(cls, irow):
        error_dict = super().invalid(irow)
        errorval = error_dict[cls.ERRORVAL]
        errorval.update({Fields.IN_FILE: irow[1][Fields.IN_FILE]})
        return error_dict

    @classmethod
    def reversion(cls, irow, irow_pre):
        error_dict = super().reversion(irow, irow_pre)
        errorval = error_dict[cls.ERRORVAL]
        prior_ts, curr_ts = errorval[cls.PRIOR_TS], errorval[cls.CURR_TS]
        prior_ts.update({Fields.IN_FILE: irow_pre[1][Fields.IN_FILE]})
        curr_ts.update({Fields.IN_FILE: irow[1][Fields.IN_FILE]})
        return error_dict


class TimeSeriesCheckTask(enriched.TimeSeriesCheckTask, SubCheckTask):
    TITLE = TS_TITLE

    INVALID_FILES = 'invalid_files'
    INVALID_DETAILS = 'file'
    INVALID_FILENAME = 'filename'
    INVALID_ERRMSG = 'error'

    def __init__(self, args):
        super().__init__(args, SeriesChecker)

    @property
    def xml_file(self):
        return self.args.tsxml

    @property
    def html_file(self):
        return self.args.tshtml

    def split_html(self, html, size_limit):
        # body_xpath = XPathBuilder.find_expr(relation=XPathBuilder.DESCENDANT, tag=BODY)
        # table_xpath = XPathBuilder.find_expr(relation=XPathBuilder.DESCENDANT, tag=TABLE)
        # yield from split_from_element(html, body_xpath, table_xpath, size_limit)
        yield get_str(html)

    def update_invalid_files(self, value: pd.Series, root):
        root = to_elementtree(root).getroot()

        if len(root) == 0 or root[0].tag != self.INVALID_FILES:
            root.insert(0, validate_element(self.INVALID_FILES))

        rcsv_addto_element(pd.DataFrame(list(value.items()),
                                        [self.INVALID_DETAILS] * len(value),
                                        [self.INVALID_ERRMSG, self.INVALID_FILENAME]),
                           root[0])


class CsvCheckTask(fmtask.FrontMonthCheckTask):

    def __init__(self):
        super().__init__(DataAccessor, CsvTaskArguments)
        self.subtasks = {self.BAR_CHECK: BarCheckTask(self.args),
                         self.TIMESERIESE_CHECK: TimeSeriesCheckTask(self.args)}
        self.source_config_map = {self.args.source_names.cme_qtg: CME_CONFIGS,
                                  self.args.source_names.cme_reactor: CME_CONFIGS,
                                  self.args.source_names.china_reactor: CHINA_CONFIGS,
                                  self.args.source_names.eurex_reactor: EUREX_CONFIGS,
                                  self.args.source_names.ose_reactor: OSE_CONFIGS,
                                  self.args.source_names.asx_reactor: ASX_CONFIGS}

    @property
    def fixed_accessor_args(self):
        return {Tags.TYPE: 'F',
                Tags.CLOCK_TYPE: 'M'}

    def update_invalid_files(self, data, check_xmls):
        if data.invalid_files is not None:
            ts_subtask = self.subtasks[self.TIMESERIESE_CHECK]
            if self.TIMESERIESE_CHECK not in check_xmls:
                check_xmls[self.TIMESERIESE_CHECK] = ts_subtask.xml_etree
            ts_subtask.update_invalid_files(data.invalid_files, check_xmls[self.TIMESERIESE_CHECK])

    def check_integrated(self, data, check_xmls):
        self.update_invalid_files(data, check_xmls)
        return super().check_integrated(data, check_xmls)

    def run_report_task(self, **kwargs):
        check_xmls = {check: self.subtasks[check].xml_etree for check in self.args.check}

        contracts = self.get_continuous_contracts(**kwargs)
        self.update_invalid_files(contracts, check_xmls)
        xmls = self.check_contracts(contracts, check_xmls, **kwargs)

        htmls = {}
        for check, xml in xmls.items():
            html = to_styled_xml(xml)
            htmls[check] = html

            if self.args.xml:
                write_etree(xml, self.subtasks[check].xml_file)
            if self.args.html:
                write_etree(html, self.subtasks[check].html_file, method='html')

        return htmls

    def email_reports(self, reports, *args):
        with EmailSession(*self.args.login) as session:
            for check, htmls in reports.items():
                status = all(self.subtasks[check].html_status(html) for html in htmls)
                status = 'PASS' if status else 'FAIL'
                title = self.subtasks[check].email_title(status, *args)
                split_funcs = self.subtasks[check].split_html
                session.email_html(self.args.recipients, htmls, title, split_funcs)

    def run(self):
        self.set_taskargs(True)
        rtype, rtime = self.args.report_config
        sources = self.args.source
        # self.set_taskargs(**{self.args.DTFROM: rtime[0], self.args.DTTO: rtime[1]})

        for src in sources:
            reports = defaultdict(list)
            products = to_iter(self.args.product, dtype=str)
            src_specific_configs = self.source_config_map[src]
            for prod in [products] if self.args.consolidate else products:
                self.set_taskargs(**{
                    self.args.SOURCE: src,
                    self.args.PRODUCT: prod,
                    self.args.DTFROM: rtime[0],
                    self.args.DTTO: rtime[1],
                    self.args.WINDOW: src_specific_configs.window,
                    self.args.WINDOW_TZ: src_specific_configs.window_tz,
                    self.args.SCHEDULE: src_specific_configs.schedule})  # temp solution
                htmls = self.run_report_task(**self.fixed_accessor_args)
                for check, html in htmls.items():
                    reports[check].append(html)

            if self.args.email:
                self.email_reports(reports, self.args.REPORT_TIME_FMT[rtype](*rtime),
                                   f'({self.args.source_report_map[src]})')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    set_dbconfig(SERVERNAME)

    task = CsvCheckTask()
    task.run()
    # products = ['ES', 'NQ', 'YM', 'ZN', 'ZB', 'UB', '6E', '6J', '6B', 'GC', 'CL']
    # task.set_taskargs(True)
    # task.email_reports({task.TIMESERIESE_CHECK: ['reports/html/QTG.20190217-20190222.series_check.html']})
