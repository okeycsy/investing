import unittest

import hood_monitor as hm


FORM4_XML = """
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerName>Jane Insider</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>Chief Financial Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionCoding>
        <transactionCode>P</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>250.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionCoding>
        <transactionCode>S</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>200</value></transactionShares>
        <transactionPricePerShare><value>300.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionCoding>
        <transactionCode>A</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>50</value></transactionShares>
        <transactionPricePerShare><value>0</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionCoding>
        <transactionCode>G</transactionCode>
      </transactionCoding>
      <transactionAmounts>
        <transactionShares><value>999</value></transactionShares>
        <transactionPricePerShare><value>1</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


class SecForm4Test(unittest.TestCase):
    def test_find_form4_xml_url_prefers_real_xml_file(self):
        index_html = """
        <a href="/ixviewer/doc/action?doc=abc.xml">ix viewer</a>
        <a href="/Archives/edgar/data/1/abc/R1.htm">R file</a>
        <a href="primary_doc.xml">primary xml</a>
        """

        url = hm._find_form4_xml_url(index_html, "https://www.sec.gov/Archives/edgar/data/1/abc/index.htm")

        self.assertEqual(url, "https://www.sec.gov/Archives/edgar/data/1/abc/primary_doc.xml")

    def test_parse_form4_xml_classifies_market_transactions_and_skips_gifts(self):
        trades = hm.parse_form4_xml(FORM4_XML, "2026-07-10", "https://example.com/form4.xml")

        self.assertEqual([t.trade_type for t in trades], ["Purchase", "Sale", "Award"])
        self.assertEqual(trades[0].filer, "Jane Insider")
        self.assertEqual(trades[0].title, "Chief Financial Officer")
        self.assertEqual(trades[0].shares, 100)
        self.assertEqual(trades[0].price, 250.50)
        self.assertEqual(trades[0].total_value, 25050.0)
        self.assertEqual(trades[1].txn_code, "S")
        self.assertEqual(trades[2].total_value, 0.0)

    def test_parse_form4_xml_returns_empty_for_malformed_xml(self):
        self.assertEqual(hm.parse_form4_xml("<ownershipDocument>", "2026-07-10", "url"), [])


if __name__ == "__main__":
    unittest.main()
