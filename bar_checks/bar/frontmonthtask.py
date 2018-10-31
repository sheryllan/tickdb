from bar.enrichedOHLCVN import *


class FrontMonthCheckTask(CheckTask):
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




    def run(self, **kwargs):
        barhtml, tshtml = self.run_checks(**kwargs)
        if self.task_email:



if __name__ == '__main__':
    task = FrontMonthCheckTask()
    logging.basicConfig(level=logging.INFO)
    # products = ['ZF', 'ZN', 'TN', 'ZB', 'UB', 'ES', 'NQ', 'YM', 'EMD', 'RTY', '6A', '6B', '6C', '6E', '6J', '6M', '6N',
    #             '6S', 'BTC', 'GC', 'SI', 'HG', 'CL', 'HO', 'RB']
    # products = ['ZF', 'ZN']
    # task.run_checks(product=products, ptype=('F', 'O'), dtfrom=dt.date(2018, 7, 1), schedule='CMESchedule')
    task.run_checks(schedule='CMESchedule')
    task.email([task.task_barhtml, task.task_tshtml], [BAR_TITILE, TS_TITLE])