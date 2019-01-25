from bs4 import BeautifulSoup
from bs4.element import Tag
import os
import premailer
import smtplib
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


def extend_multiple(parent, children):
    for child in children:
        parent.append(child)
    return parent


def find_all_by_depth(root, tag, depth=1, **kwargs):
    if isinstance(root, Tag):
        if depth <= 1:
            yield from root.find_all(tag, recursive=False, **kwargs)
        else:
            for child in root.children:
                yield from find_all_by_depth(child, tag, depth - 1, **kwargs)


def split_tags(tags, buffer_size, grouping=lambda x: x):
    for grouped in func_grouper(grouping(tags), buffer_size, lambda x: len(str(x)), iter):
        yield flatten_iter(grouped, excl_types=(str, Tag))


def extracted_trees(nodes):
    for node in nodes:
        yield node.parent
        node.extract()


def split_html(html, parent_func, desc_func, buffer_size, split_func, prettify=False):
    soup = BeautifulSoup(html, 'html.parser')
    nodes = list(parent_func(soup))
    trees = list(extracted_trees(nodes))
    buffer_init = buffer_size - len(str(soup))

    def to_string():
        return str(soup) if not prettify else soup.prettify()

    for node, tree in zip(nodes, trees):
        buffer = buffer_init - len(str(soup.new_tag(node.name)))
        for descendants in split_func(desc_func(node), buffer):
            node_new = extend_multiple(soup.new_tag(node.name), descendants)
            tree.append(node_new)
            yield to_string()
            node_new.decompose()


def split_elements(html, tag, buffer_size, prettify=False, **attribs):
    pass


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
