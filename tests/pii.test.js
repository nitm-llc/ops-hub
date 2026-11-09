// PII scrubbing. cxScrubPII feeds the Fin export; scrubForAudit feeds the CX
// scorecard audit (Claude API + D1). Both must fail closed on identity AND keep
// the words the reader needs (product names, our links). Invented examples only.
import { test } from "node:test";
import assert from "node:assert/strict";
import { cxScrubPII, scrubForAudit, scrubCodes, scrubNames } from "../src/pii.js";

test("cxScrubPII: emails, phones, addresses, postcodes, foreign links", () => {
  const out = cxScrubPII("Mail jane.doe+x@example.com or call (555) 123-4567. Ship to 42 Oak Lane, Apt 3, Austin 78701. See https://evil.example/x and https://anurseinthemaking.com/products/x");
  assert.doesNotMatch(out, /jane|555|Oak|78701|evil/);
  assert.match(out, /\[email\]/);
  assert.match(out, /\[phone\]/);
  assert.match(out, /https:\/\/anurseinthemaking\.com\/products\/x/);
});

test("scrubForAudit: eBook codes, one-time codes, discount codes", () => {
  assert.equal(scrubForAudit("Your eBook code is ABCD-EFGH-1234."), "Your eBook code is [code].");
  assert.equal(scrubForAudit("My verification code is 482913"), "My verification code is [one-time code]");
  assert.equal(scrubForAudit("Use code NURSE15 for 15% off"), "Use code [discount code] for 15% off");
  assert.equal(scrubForAudit("promo code: save10"), "promo code: [discount code]");
  assert.equal(scrubForAudit("Here it is: 7Y3K9QPL2"), "Here it is: [code]");
  assert.equal(scrubForAudit("reply with 093311 please"), "reply with [code] please");
});

test("scrubForAudit: order numbers become a placeholder, not a hole", () => {
  assert.equal(scrubForAudit("Order number is #2-123456"), "Order number is [order number]");
  assert.equal(scrubForAudit("it was #10423"), "it was [order number]");
});

test("scrubForAudit: keeps product names and ordinary words", () => {
  const s = "The Complete Nursing School Bundle, The NCLEX Study Notebook, NCLEX-RN, eBook on VitalSource Bookshelf, N95, 2-day shipping, 15% off";
  assert.equal(scrubForAudit(s), s);
});

test("scrubNames: keeps first names, drops surnames", () => {
  assert.equal(scrubNames("Hi Jane, thanks. Jane Okafor", ["JANE OKAFOR"]), "Hi Jane, thanks. Jane [surname]");
  assert.equal(scrubNames("Name: Pat Smith-Lee\nEmail: x", []), "Name: Pat [surname]\nEmail: x");
  assert.equal(scrubNames("Hi Al Bo", ["Al Bo"]), "Hi Al Bo"); // tokens under 3 chars are left alone
});

test("scrubCodes leaves prose without codes alone", () => {
  const s = "I passed my exam! Thank you so much, the code of conduct section helped.";
  assert.equal(scrubCodes(s), s);
});
