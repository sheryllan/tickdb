from collections.abc import Mapping
from lxml import etree

from commonlib import *


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


def df_to_etree(df, root, row_ele=None, index_name=False):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    xml = validate_element(root)
    for i, row in df.iterrows():
        subelement = etree.SubElement(xml, i if row_ele is None else row_ele)
        if index_name is not False:
            subelement.set(df.index.name if index_name is None else index_name, str(i))
        rcsv_addto_etree(row, subelement)

    return xml


def rcsv_addto_etree(value, root, **kwargs):
    if isinstance(root, str):
        root = etree.Element(root)
    elif not etree.iselement(root):
        raise TypeError('Invalid type of root: it must be either a string or etree.Element')

    if isinstance(value, (Mapping, pd.Series)):
        for fk, fv in value.items():
            if nontypes_iterable(fv):
                root.append(rcsv_addto_etree(fv, fk, **kwargs))
            else:
                root.set(fk, str(fv))
    elif isinstance(value, pd.DataFrame):
        df_to_etree(value, root, **kwargs)
    elif nontypes_iterable(value):
        for val in value:
            rcsv_addto_etree(val, root, **kwargs)
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


def write_etree(element, outpath=None, pis=None, xml_declaration=True, method='xml', encoding='utf-8',
                append=False, decode=False):
    if outpath is None:
        return None
    tree = to_elementtree(element, pis)
    text = etree.tostring(tree, xml_declaration=xml_declaration, method=method, encoding=encoding, pretty_print=True)
    if text is not None:
        mode = 'ab+' if append else 'wb+'
        with open(outpath, mode) as stream:
            stream.write(text)
    return text.decode(encoding) if decode else text


def to_styled_xml(xml, xsl=None):
    doc = xml
    if isinstance(xml, str):
        doc = etree.parse(xml)
    elif etree.iselement(xml):
        doc = etree.ElementTree(xml)

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
        return '[{}]'.format(value)

    @classmethod
    def evaluation(cls, obj, value='', is_attrib=False):
        if not obj:
            raise ValueError("'obj' argument must not be empty or None")

        obj = '@' + obj if is_attrib else obj
        return cls.selector("{}='{}'".format(obj, value) if value else obj)

    @classmethod
    def find(cls, start_tag=CURRENT, relation=CHILD, tag=ALL, selector=None):
        return ''.join(filter(None, [start_tag, relation, tag, selector]))
