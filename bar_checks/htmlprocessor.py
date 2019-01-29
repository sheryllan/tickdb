from bs4 import BeautifulSoup
from bs4.element import Tag
import os
import premailer
import smtplib
from itertools import groupby

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from contextlib import AbstractContextManager
from xmlconverter import *


BODY = 'body'

TABLE = 'table'
TBODY = 'tbody'
TR = 'tr'
TH = 'th'

COLSPAN = 'colspan'


def level_order_traverse(element, targets):
    level_dict = {}
    found_list, targets = [], set(targets)

    el_queue, dict_queue = list(element), [level_dict]*len(element)
    while len(el_queue) > 0:
        el_curr = el_queue.pop(0)
        dict_curr = dict_queue.pop(0)
        if len(found_list) == len(targets):
            dict_curr[el_curr] = False
        elif el_curr in targets:
            found_list.append(el_curr)
            dict_curr[el_curr] = True
        else:
            children = list(el_curr)
            if not children:
                dict_curr[el_curr] = None
            else:
                new_dict = {}
                dict_curr[el_curr] = new_dict
                el_queue.extend(children)
                dict_queue.extend([new_dict] * len(children))

    return level_dict


def get_len(element):
    return len(etree_to_str(element, xml_declaration=False, method='html'))


def target_insert_pos(el_dict, parent):
    parent[:] = []
    i = 0
    for is_target, el_group in groupby(el_dict, lambda x: el_dict[x] is True):
        if is_target:
            for el in el_group:
                yield el, i
        else:
            el_group = list(el_group)
            parent[:] = list(parent) + el_group
            i += len(el_group)


def split_from_element(html, el_xpath, desc_xpath, buffer_size, grouping=lambda x: [x]):

    def rcsv_split(parent, el_dict, parent_buffer):
        if not isinstance(el_dict, dict):
            return iter('')

        target_pos = dict(target_insert_pos(el_dict, parent))
        if not target_pos:
            for child in parent:
                yield from rcsv_split(child, el_dict[child], parent_buffer)
        else:
            buffer = parent_buffer - get_len(parent)
            children_orig = list(parent)
            for chunk in func_grouper(grouping(target_pos.keys()), buffer, lambda x: sum(map(get_len, x))):
                for k, child in enumerate(flatten_iter(chunk, excl_types=(str, etree._Element))):
                    parent.insert(target_pos[child] + k, child)
                yield etree_to_str(html, xml_declaration=False, method='html', decode=True)
                parent[:] = children_orig

    html = to_elementtree(html)
    element = html.find(el_xpath)
    if element is None:
        yield etree_to_str(html, xml_declaration=False, method='html', decode=True)
    else:
        desc_elements = element.findall(desc_xpath)
        if not desc_elements:
            yield etree_to_str(html, xml_declaration=False, method='html', decode=True)
        else:
            level_dict = level_order_traverse(element, desc_elements)
            buffer_init = buffer_size - get_len(html) + get_len(element)
            yield from rcsv_split(element, level_dict, buffer_init)


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
    def email_html(self, recipients, contents, subject, splitfunc=lambda x, y: x):
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
            self.smtp.sendmail(self.user, msg_to, msg.as_string())

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.smtp.quit()
        return False
