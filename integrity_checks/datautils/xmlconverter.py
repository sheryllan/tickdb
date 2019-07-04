from collections.abc import Mapping
from lxml import etree
import pandas as pd

from ..commonlib import source_from
from pythoncore.commonlib.iterations import *


XSL_PI_TEXT_FMT = 'type="text/xsl" href="{}"'
XSL_PI_TARGET = 'xml-stylesheet'


def xsl_pi(xsl):
    return etree.ProcessingInstruction(XSL_PI_TARGET, XSL_PI_TEXT_FMT.format(xsl))


def validate_element(element):
    if isinstance(element, str):
        element = etree.Element(element)
    elif not etree.iselement(element):
        raise TypeError('Invalid type of root: it must be either a string or etree.Element')
    return element


def df_to_element(df, root, row_ele=None, index_name=False):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    xml = validate_element(root)
    for i, row in df.iterrows():
        subelement = etree.SubElement(xml, i if row_ele is None else row_ele)
        if index_name is not False:
            subelement.set(df.index.name if index_name is None else index_name, str(i))
        rcsv_addto_element(row, subelement)

    return xml


def rcsv_addto_element(value, root, **kwargs):
    if isinstance(root, str):
        root = etree.Element(root)
    elif not etree.iselement(root):
        raise TypeError('Invalid type of root: it must be either a string or etree.Element')

    if isinstance(value, (Mapping, pd.Series)):
        for fk, fv in value.items():
            if nontypes_iterable(fv):
                root.append(rcsv_addto_element(fv, fk, **kwargs))
            else:
                root.set(fk, str(fv))
    elif isinstance(value, pd.DataFrame):
        df_to_element(value, root, **kwargs)
    elif nontypes_iterable(value):
        for val in value:
            rcsv_addto_element(val, root, **kwargs)
    else:
        root.text = str(value)

    return root


def to_elementtree(root, pis=None):
    if etree.iselement(root):
        tree = etree.ElementTree(root)
    elif isinstance(root, str):
        tree = etree.fromstring(source_from(root)).getroottree()
    else:
        tree = root

    if pis is not None:
        pis = [pis] if etree.iselement(pis) else pis
        element = tree.getroot()
        for pi in pis:
            element.addprevious(pi)
    return tree


def etree_to_str(element, pis=None, xml_declaration=True, method='xml', encoding='utf-8', decode=False):
    tree = to_elementtree(element, pis)
    text = etree.tostring(tree, xml_declaration=xml_declaration, method=method, encoding=encoding, pretty_print=True)
    return text.decode(encoding) if decode else text


def write_etree(element, outpath=None, pis=None, xml_declaration=True, method='xml', encoding='utf-8',
                append=False):
    if outpath is None:
        return None

    text = etree_to_str(element, pis, xml_declaration, method, encoding)
    if text is not None:
        mode = 'ab+' if append else 'wb+'
        with open(outpath, mode) as stream:
            stream.write(text)


def to_styled_xml(xml, xsl=None):
    doc = to_elementtree(xml)
    if xsl is None:
        pi = find_first_n(doc.xpath(XPathBuilder.PI), lambda x: x.target == XSL_PI_TARGET)
        if not etree.iselement(pi):
            raise ValueError('xsl files not specified')
        xsl = pi.get('href')
    transform = etree.XSLT(etree.parse(xsl))
    return transform(doc)


class XPathBuilder(object):

    CURRENT = '.'
    PARENT = '..'
    CHILD = '/'
    DESCENDANT = '//'
    ALL = '*'

    PI = '//processing-instruction()'

    @classmethod
    def selector(cls, value):
        return "[{}]".format(value)

    @classmethod
    def expression(cls, is_attrib=False, **kwargs):
        evaluation = r"{}='{}'"
        return ''.join(cls.selector(evaluation.format('@' + k if is_attrib else k, v)) for k, v in kwargs.items())

    @classmethod
    def find_expr(cls, start_tag=CURRENT, relation=CHILD, tag=ALL, selector=None):
        expr = ''.join(filter(None, [start_tag, relation, tag, selector]))
        return expr