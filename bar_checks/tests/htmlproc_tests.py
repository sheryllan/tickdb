import unittest as ut
from htmlprocessor import *
from itertools import groupby


class HtmlTests(ut.TestCase):
    def test_split_html(self):
        html ="""<html>
  <head>
    <style media="screen" type="text/css">
					dl dt { font-weight: bold; padding: 7px 0;}
					dl dd { display:inline-block; }
					dl dd:before { content: '\00a0\2022\00a0\00a0'; color:#999;	color:rgba(0,0,0,0.5); font-size:11px; }
					
					table { border-collapse:collapse; text-align:center; font-family:"Lucida Grande","Lucida Sans Unicode","Bitstream Vera Sans","Trebuchet MS",Verdana,sans-serif}
					table th, td { padding:3px 7px 2px; font-size: small; border: 2px solid #f2f2f2; }
					table caption { text-align: left; }
					
					th.lv1 { color: #000000; background-color: #888888; text-align:center; }
					th.lv2 { color: #000000; background-color: #d9d9d9; text-align:left; }
					
					th.lv1 { background-color: #888888; padding:3px 7px; }
					th.lv2 { background-color: #d9d9d9; padding:2px 5px; }
					
					
					td.good { color: #00AA00; font-weight: bold; font-size: small; }
					td.bad { color: #AA0000; font-weight: bold; font-size: small; }
					
					pre.detail { font-weight: normal; }
					pre.warning { color: #cccc00; font-weight: normal; }
				</style>
  </head>
  <body>
    <table>
      <caption>
						Time: 2018-07-01 00:00:00 to 2018-10-24 00:00:00</caption>
      <thead>
        <tr>
          <th class="lv1" id="time" scope="col">time</th>
          <th class="lv1" id="prices_rollover_check" scope="col">prices_rollover_check</th>
          <th class="lv1" id="high_low_check" scope="col">high_low_check</th>
          <th class="lv1" id="pclv_order_check" scope="col">pclv_order_check</th>
          <th class="lv1" id="bid_ask_check" scope="col">bid_ask_check</th>
          <th class="lv1" id="volume_check" scope="col">volume_check</th>
          <th class="lv1" id="vol_on_lv1_check" scope="col">vol_on_lv1_check</th>
          <th class="lv1" id="vol_on_lv2_check" scope="col">vol_on_lv2_check</th>
          <th class="lv1" id="vol_on_lv3_check" scope="col">vol_on_lv3_check</th>
          <th class="lv1" id="vwap_check" scope="col">vwap_check</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th id="5237679648093372536" class="span lv2" colspan="10" scope="colgroup">
				Bar: PRODUCT=ES, TYPE=F, EXPIRY=JUN2019, CLOCK_TYPE=M, WIDTH=60, OFFSET=0</th>
        </tr>
        <tr>
          <td>2018-07-02 00:00:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        <tr>
          <td>2018-07-02 01:00:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        <tr>
          <td>2018-07-02 02:00:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined
lbid undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask1 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined
cbid1 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask1 undefined
cbid1 undefined
cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined
lbid undefined
lask undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        <tr>
          <td>2018-07-02 03:00:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined
lbid undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined
lbid undefined
lask undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        <tr>
          <td>2018-07-02 04:00:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined
lbid undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined
lbid undefined
lask undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        <tr>
          <td>2018-07-02 05:00:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        <tr>
          <th id="1559154486344075029" class="span lv2" colspan="10" scope="colgroup">
				Bar: PRODUCT=ES, TYPE=F, EXPIRY=JUN2019, CLOCK_TYPE=M, WIDTH=60, OFFSET=10</th>
        </tr>
        <tr>
          <td>2018-07-02 00:10:00+00:00</td>
          <td class="good">Passed</td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">low undefined
high undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cask2 undefined
cask3 undefined
cbid2 undefined
cbid3 undefined</pre>
            </details>
          </td>
          <td class="good">
            <details>
              <summary>Passed</summary>
              <pre class="warning">cbid2 undefined
cask2 undefined
cbid3 undefined
cask3 undefined</pre>
            </details>
          </td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
          <td class="good">Passed</td>
        </tr>
        </tbody>
    </table>
  </body>
</html>"""

        def grouping(tags):

            def is_bar_row(tr):
                th = tr.find(TH, recursive=False)
                return th is not None and int(th[COLSPAN]) > 1

            bar = []
            for is_bar, group in groupby(tags, is_bar_row):
                if is_bar:
                    bar = list(group)
                else:
                    bar.extend(group)
                    yield bar
                    bar = []



        for split in split_html(html, lambda x: x.find_all(TBODY), lambda x: find_all_by_depth(x, TR), 5000,
                                lambda x, y: split_tags(x, y, grouping), True):
            print(split)