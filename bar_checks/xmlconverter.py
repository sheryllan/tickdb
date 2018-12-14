from collections.abc import Mapping
from os import linesep
from lxml import etree

from commonlib import *


def validate_element(element):
    if isinstance(element, str):
        element = etree.Element(element)
    elif not etree.iselement(element):
        raise TypeError('Invalid type of root: it must be either a string or etree.Element')
    return element


def pd_to_etree(df, root, row_ele=None, index_name=False):
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    # def subelements():
    #     for i, row in df.iterrows():
    #         subelement = etree.SubElement(xml, row_ele)
    #         if index_name is not None:
    #             subelement.set(index_name, str(i))
    #         rcsv_addto_etree(row, subelement)
    #
    #         yield subelement

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
        pd_to_etree(value, root, **kwargs)
    elif nontypes_iterable(value):
        for val in value:
            rcsv_addto_etree(val, root, **kwargs)
    else:
        root.text = str(value)

    return root


def etree_tostr(element, outpath=None, xsl=None, header=None, encoding='utf-8'):
    strings = []

    xml_header = '<?xml version="1.0" encoding="{}"?>'.format(encoding)
    if header == 'xml':
        strings.append(xml_header)
        if xsl is not None:
            xsl_pi = '<?xml-stylesheet type="text/xsl" href="{}"?>'.format(xsl)
            strings.append(xsl_pi)

    content = etree.tostring(element, pretty_print=True)
    if content is not None:
        content = content.decode(encoding)
        strings.append(content)

    text = linesep.join(strings)
    if outpath is not None:
        with open(outpath, 'w+', encoding=encoding) as stream:
            stream.write(text)

    return text


def to_styled_xml(xml, xsl):
    doc = xml
    if not etree.iselement(doc):
        doc = etree.parse(xml)
    transform = etree.XSLT(etree.parse(xsl))
    return transform(doc)
