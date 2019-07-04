import os
import smtplib
from contextlib import AbstractContextManager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import groupby

import premailer

from .xmlconverter import *

BODY = 'body'

TABLE = 'table'
TBODY = 'tbody'
TR = 'tr'
TH = 'th'

COLSPAN = 'colspan'


def get_str(element):
    return etree_to_str(element, xml_declaration=False, method='html', decode=True)


def extract_target_pos(parent, is_target_func):
    children = list(parent)
    parent[:] = []
    other_children = list(parent)
    i = 0
    for is_target, target_group in groupby(children, is_target_func):
        if is_target:
            for el in target_group:
                yield el, i
        else:
            target_group = list(target_group)
            other_children = other_children + target_group
            i += len(target_group)

    parent[:] = other_children


def split_from_element(html, el_xpath, desc_xpath, buffer_size, grouping=lambda x: [x]):

    def rcsv_split(parent, target_ids, parent_buffer):
        if not target_ids:
            yield get_str(html)
            return

        target_pos = dict(extract_target_pos(parent, lambda x: id(x) in target_ids))
        if not target_pos:
            for child in parent:
                yield from rcsv_split(child, target_ids, parent_buffer)
        else:
            buffer = parent_buffer - len(get_str(parent))
            other_children = list(parent)
            for chunk in func_grouper(grouping(target_pos.keys()), buffer, lambda x: sum(map(len, map(get_str, x)))):
                for k, tchild in enumerate(flatten_iter(chunk, excl_types=(str, etree._Element))):
                    parent.insert(target_pos[tchild] + k, tchild)
                    target_ids.remove(id(tchild))
                yield get_str(html)
                parent[:] = other_children

    html = to_elementtree(html)
    element = html.find(el_xpath)
    if element is None:
        yield get_str(html)
        return

    desc_ids = set(map(id, element.findall(desc_xpath)))
    buffer_init = buffer_size - len(get_str(html)) + len(get_str(element))
    yield from rcsv_split(element, desc_ids, buffer_init)


class EmailSession(AbstractContextManager):
    ATTACHMENT_LIMIT = 20000000

    def __init__(self, user, password, server='smtp.gmail.com'):
        self.user = user
        self.password = password
        self.server = server
        self.smtp = smtplib.SMTP_SSL(self.server)

    def login(self):
        self.smtp.login(self.user, self.password)

    # contents must be an iterable of str or etree._ElementTree
    def email_html(self, recipients, contents, subject, splitfunc=lambda x, y: get_str(x)):
        msg_to = ', '.join(recipients)
        transformed = (splitfunc(premailer.transform(to_elementtree(c).getroot()), self.ATTACHMENT_LIMIT)
                       for c in to_iter(contents))
        for content in func_grouper(flatten_iter(transformed), self.ATTACHMENT_LIMIT, lambda x: len(x), iter):
            body = MIMEText(os.linesep.join(content), 'html')
            msg = MIMEMultipart('alternative')
            msg['From'] = self.user
            msg['To'] = msg_to
            msg['Subject'] = subject
            msg.attach(body)
            self.smtp.send_message(msg)

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.smtp.quit()
        return False
