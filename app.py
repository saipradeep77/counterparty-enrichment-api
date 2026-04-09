from flask import Flask, request, jsonify, send_from_directory, send_file
import re, os, io, pandas as pd

app = Flask(__name__, static_folder='static')

# ─────────────────────────────────────────────
# LISTS — loaded once at startup
# ─────────────────────────────────────────────
EXCLUSION_TERMS = set()      # plain string exact match (uppercased)
EXCLUSION_REGEX  = []        # compiled regex patterns from exclusion list
WHITELIST_ALIASES = {}       # alias_upper -> {canonical, sector}

def load_lists():
    global EXCLUSION_TERMS, EXCLUSION_REGEX, WHITELIST_ALIASES
    excl_path = os.environ.get('EXCLUSION_LIST_PATH', 'data/Counterparty_Search_Exclusion_List_v9_01_apr.xlsx')
    wl_path   = os.environ.get('WHITELIST_PATH',      'data/counterparty_whitelist_v8_01_apr.xlsx')

    if os.path.exists(excl_path):
        df = pd.read_excel(excl_path)
        df.columns = [c.strip() for c in df.columns]
        terms = df[df['Category'].notna()]['Exclusion Term'].dropna().str.strip()
        plain_terms = set()
        regex_patterns = []
        REGEX_INDICATOR = set(['^', '[', ']', '{', '}', '+'])
        for t in terms:
            if any(c in t for c in REGEX_INDICATOR):
                try:
                    regex_patterns.append(re.compile(t, re.IGNORECASE))
                except Exception:
                    plain_terms.add(t.upper())
            else:
                plain_terms.add(t.upper())
                plain_terms.add(re.sub(r'[()]', '', t).upper().strip())
        EXCLUSION_TERMS = plain_terms
        EXCLUSION_REGEX = regex_patterns
        print(f"Loaded {len(EXCLUSION_TERMS)} exclusion terms + {len(EXCLUSION_REGEX)} regex patterns (v9)")
    else:
        print(f"WARNING: Exclusion list not found at {excl_path}")

    if os.path.exists(wl_path):
        df = pd.read_excel(wl_path)
        df.columns = [c.strip() for c in df.columns]
        for _, row in df.iterrows():
            alias = str(row.get('Alias','')).strip().upper()
            if alias and alias != 'NAN':
                WHITELIST_ALIASES[alias] = {
                    'canonical': str(row.get('Canonical Name','')).strip(),
                    'sector':    str(row.get('Sector','')).strip()
                }
        print(f"Loaded {len(WHITELIST_ALIASES)} whitelist aliases (v8)")
    else:
        print(f"WARNING: Whitelist not found at {wl_path}")

# ─────────────────────────────────────────────
# EXTRACTION HELPERS
# ─────────────────────────────────────────────
def strip_location(text):
    # City, State ZIP Country  (e.g. ", Portland, OR 97214 US")
    text = re.sub(r',\s*[\w\s\.\-]+,\s*[A-Z]{2}\s*\d{0,5}\s*\d{0,4}\s*(US|CA|MX|UK)?\s*$', '', text).strip()
    # State ZIP Country without city  (e.g. " OR 97214 US")
    text = re.sub(r'\s+[A-Z]{2}\s+\d{5}(?:\d{4})?\s+(US|CA|MX|UK)\s*$', '', text).strip()
    # Bare ", City, ST US" without zip
    text = re.sub(r',\s*[\w\s\.\-]+,\s*[A-Z]{2}\s+(US|CA|MX|UK)\s*$', '', text).strip()
    # Phone numbers anywhere: 888-802-3080, 508-4852020, (800) 331-0500, 8004561234
    text = re.sub(r',?\s*\(?\d{3}\)?[-.\s]?\d{3,4}[-.\s]?\d{4}', '', text).strip()
    # Trailing ", ST" or ", ST US"
    text = re.sub(r',\s*[A-Z]{2}\s*(US|CA|MX)?\s*$', '', text).strip()
    return text.strip().rstrip(',').strip()

def strip_noise(text):
    # Masked SSN / account numbers: XXXXX1234, *****3237, XXXXXXXXX200909
    text = re.sub(r'\s+[X*]{4,}\d+[-\d]*\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\s+\*{4,}\d+\s*$', '', text).strip()
    # Trailing PRENOTE keyword
    text = re.sub(r'\s+PRENOTE\s*$', '', text, flags=re.IGNORECASE).strip()
    # Trailing CCD, PPD, WEB keywords
    text = re.sub(r'\s+(CCD|PPD|WEB)\s*$', '', text, flags=re.IGNORECASE).strip()
    # Trailing alphanumeric reference codes (8+ chars, must contain at least one digit)
    text = re.sub(r'\s+(?=[A-Z0-9]*\d)[A-Z0-9]{8,}\s*$', '', text).strip()
    # Trailing long numbers (6+ digits)
    text = re.sub(r'\s+\d{6,}\s*$', '', text).strip()
    # Trailing short reference codes like AB-1234, WEB123456
    text = re.sub(r'\s+[A-Z]{2,4}-?\d{4,}\s*$', '', text).strip()
    # Trailing x+digits pattern (e.g. "x12345", "xx1234")
    text = re.sub(r'\s+x+\d+\s*$', '', text, flags=re.IGNORECASE).strip()
    # Trailing mixed-case alphanumeric codes — must contain at least one digit (e.g. "dffd66669837425", "IW8N48XGB")
    text = re.sub(r'\s+(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{8,}\s*$', '', text).strip()
    # Trailing 4-5 digit numbers (store/ref numbers at end)
    text = re.sub(r'\s+\d{4,5}\s*$', '', text).strip()
    # Dollar amounts (e.g. "$33.71", "$-40.00")
    text = re.sub(r',?\s*\$-?[\d,.]+\s*(Surcharge|Cash\s*Back)?\s*$', '', text, flags=re.IGNORECASE).strip()
    # URLs
    text = re.sub(r',?\s*https?://\S*', '', text, flags=re.IGNORECASE).strip()
    return text.strip().rstrip(',').strip()

def clean_counterparty(text):
    """Final cleanup pass on extracted counterparty name — runs on every result."""
    if not text or text == 'UNKNOWN':
        return text
    # Strip store/location numbers: "#1185", "# 07861", "#12000984"
    text = re.sub(r'\s*#\s*\d+\s*$', '', text).strip()
    text = re.sub(r'\s*#\s*\d+\b', '', text).strip()  # mid-string store numbers
    # Strip bare trailing 4-5 digit store numbers after merchant name
    text = re.sub(r'\s+\d{4,5}\s*$', '', text).strip()
    # Strip short embedded alphanumeric codes (e.g. "776A8" between words)
    text = re.sub(r'\s+\d+[A-Z]\d*\s+', ' ', text).strip()
    text = re.sub(r'\s+[A-Z]\d+[A-Z]\d*\s+', ' ', text).strip()
    # Strip embedded phone numbers (10-digit: 8888081723, or formatted)
    text = re.sub(r'\s+\d{10}\b', '', text).strip()
    text = re.sub(r',?\s*\(?\d{3}\)?[-.\s]?\d{3,4}[-.\s]?\d{4}', '', text).strip()
    # Strip payment keywords that shouldn't be in the name
    text = re.sub(r'\s+(WEB PYMT|WEB PMT|EPAY|INS PREM|LOAN PYMNT|PPDPAYROLL|PPD\s*PAYROLL)\b', '', text, flags=re.IGNORECASE).strip()
    # Strip trailing PRENOTE
    text = re.sub(r'\s+PRENOTE\s*$', '', text, flags=re.IGNORECASE).strip()
    # Strip masked SSN/account: XXXXX1234, *****3237
    text = re.sub(r'\s+[X*]{3,}\d*[-\d]*\s*$', '', text, flags=re.IGNORECASE).strip()
    # Strip trailing alphanumeric reference codes (CC231013417, W7210, etc.)
    # Must contain at least one digit — pure alpha words like TURBOTAX are business names
    text = re.sub(r'\s+[A-Z]{1,3}\d{4,}\s*$', '', text).strip()
    text = re.sub(r'\s+(?=[A-Z0-9]*\d)[A-Z0-9]{8,}\s*$', '', text).strip()
    # Strip trailing long digit sequences
    text = re.sub(r'\s+\d{5,}\s*$', '', text).strip()
    # Strip dollar amounts and surcharge/cash back
    text = re.sub(r',?\s*\$-?[\d,.]+\s*(Surcharge|Cash\s*Back)?\s*$', '', text, flags=re.IGNORECASE).strip()
    # Strip URLs
    text = re.sub(r',?\s*https?://\S*', '', text, flags=re.IGNORECASE).strip()
    # Strip trailing ", ST" or ", ST US" location remnants
    text = re.sub(r',\s*[A-Z]{2}\s*(US|CA|MX)?\s*$', '', text).strip()
    # Strip trailing comma
    text = text.rstrip(',').strip()
    # Strip "Cashback---" dash-delimited strings
    if re.match(r'^Cashback---', text, re.IGNORECASE):
        parts = text.split('---')
        # Try to find a merchant name in the parts
        for p in parts[1:]:
            if p and not p.isdigit():
                text = p
                break
    # Strip trailing punctuation remnants (& , . ;)
    text = re.sub(r'[\s&,;.]+$', '', text).strip()
    # Collapse multiple spaces
    text = re.sub(r'\s{2,}', ' ', text).strip()
    return text

def strip_person_name(text, pattern):
    """Strip trailing person/account-holder names from business counterparties.
    Only for patterns where the entity is a business and a person name follows."""
    if not text:
        return text
    # Patterns: "COMPANY NAME Firstname Lastname REF_CODE"
    # After strip_noise removes the ref code, we may have "COMPANY NAME Firstname Lastname"
    # We strip trailing "Firstname Lastname" if it looks like a person name after a business
    # Only apply to patterns where we know a person name follows the business
    person_patterns = {
        'RETRY_PAYMENT', 'MOBILE_PAYMENT', 'MEMBERSHIP_FEE',
        'STANDARD_ACH', 'UNRECOGNISED'
    }
    if pattern not in person_patterns:
        return text
    # Match trailing "FirstName LastName" (two capitalized words at end) after known business indicators
    # But only if the text before it looks like a business (has a keyword or suffix)
    m = re.match(r'^(.+?(?:LLC|INC|CORP|LTD|CO|BANK|CRD|PAY|INS|DIRECT|PRODUCTS)\.?)\s+[A-Z][a-z]+\s+[A-Z][a-z]+.*$', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return text

PAYMENT_RAILS = {
    'CASH APP','CASHAPP','PAYPAL','VENMO','ZELLE','APPLE PAY','GOOGLE PAY',
    'SAMSUNG PAY','SQUARE','STRIPE','CHIME','VARO','CURRENT','DAVE','BRIGIT',
    'EARNIN','KLARNA','AFTERPAY','AFFIRM','SEZZLE','SPLITIT','VISA DIRECT',
    'MASTERCARD SEND','WESTERN UNION','MONEYGRAM','REMITLY','WISE','WORLDREMIT',
    'DK','DRAFTKINGS','FANDUEL','BETMGM'
}

KNOWN_MERCHANTS = {
    'AMAZON','AMZN','WALMART','WAL-MART','TARGET','COSTCO','NETFLIX','SPOTIFY',
    'HULU','DISNEY','HBO','APPLE','GOOGLE','MICROSOFT','ADOBE','GEXA ENERGY',
    'AT&T','VERIZON','T-MOBILE','TMOBILE','COMCAST','XFINITY','SPECTRUM',
    'UBER','LYFT','DOORDASH','GRUBHUB','INSTACART','TURBOTAX','TURBO TAX',
    'INTUIT','QUICKBOOKS','TPG PRODUCTS','SBTPG','SEEDFI','CREDIT GENIE',
    'SAMSUNG','SNAPTRAVEL','PROG DIRECT','PROGRESSIVE','GEICO','ALLSTATE',
    'STATE FARM','USAA','KINDLE'
}

GOV_PATTERNS = [
    r'^STATE OF\s+\w+',r'^ST\.?\s+OF\s+\w+',r'^IRS\b',r'^Division of',
    r'^US DEPT',r'^DEPT OF',r'^CTDOL\b',r'^TN UI\b',
    r'^NY STATE\b',r'^VA DEPT\b',r'^Georgia Dep',r'^NM\s+\w+\s+COUNTY',
    r'^[A-Z]{2}\s+DEPT\s+(OF\s+)?',
    r'^[A-Z]{2}TREASURY\b',  # WVTREASURY, etc.
]

# pattern_name -> (plain_english_name, description)
PATTERN_META = {
    'PAYMENT_RAIL':          ('Payment Rail',          'Payment platform before * is a known rail (Cash App, PayPal etc.) — counterparty is the person or merchant after *'),
    'MERCHANT_WITH_CODE':    ('Merchant with Code',    'Known merchant before * — merchant is the counterparty; code after * is a transaction reference'),
    'PAYROLL_DIRECT_DEPOSIT':('Payroll Direct Deposit','Employer name appears before DIRECT DEP keyword — person name after it is the account holder, not the counterparty'),
    'PAYROLL_DEPOSIT':       ('Payroll Deposit',        'Employer or payroll processor before PAYROLL keyword — person name after is the account holder'),
    'RETURN_TRANSACTION':    ('Return Transaction',     'Helix return — bank or institution name extracted from after the from/to keyword'),
    'ACCOUNT_TRANSFER':      ('Account Transfer',       'Transfer between accounts — entity extracted between Transfer from and to'),
    'STANDARD_ACH':          ('Standard ACH',           'Standard NACHA format — entity before trailing keyword such as FUND PMTS, EFT, or ACH'),
    'HEALTHME_PLATFORM':     ('Healthme Platform',      'Healthme transaction string — trailing code is a transaction reference, not part of the name; Healthme extracted as-is'),
    'POS_CARD_TRANSACTION':  ('POS Card Transaction',   'Silverlake POS/DDA long-form string — merchant name embedded after numeric timestamp'),
    'TELEPHONE_TRANSFER':    ('Telephone Transfer',     'Silverlake telephone-initiated transfer — string contains only routing codes and account numbers, no counterparty extractable'),
    'INDIAN_EAGLE':          ('Indian Eagle',           'Indian Eagle travel agency string — CAS reference code stripped, entity extracted as-is'),
    'WALMART_VARIANT':       ('Walmart Variant',        'WAL WAL-MART duplicated prefix — store number and location stripped, entity name extracted'),
    'ACH_TRANSACTION':       ('ACH Transaction',        'ACH Credit or Debit prefix — entity extracted from the description body after the reference code'),
    'WIRE_TRANSFER':         ('Wire Transfer',          'IMAD/OMAD wire format — entity extracted from the wire description after the reference number'),
    'SQUARE_MERCHANT':       ('Square Merchant',        'Square POS transaction — merchant name extracted after SQ *'),
    'NNT_MERCHANT':          ('NNT Merchant',           'NNT-prefixed string — merchant extracted between NNT prefix and numeric location code'),
    'SY_MERCHANT':           ('SY Merchant',            'SY#-prefixed string — merchant extracted between SY# prefix and alphanumeric location code'),
    'VENDOR_PAYMENT':        ('Vendor Payment',         'Entity before VENDOR PMT keyword is the counterparty'),
    'TRANSACTION_FEE':       ('Transaction Fee',        'Entity before TRAN FEE keyword — numeric reference between entity and keyword is stripped'),
    'MOBILE_PAYMENT':        ('Mobile Payment',         'Entity before MOBILE PMT keyword — person name after is the account holder making the payment'),
    'RETRY_PAYMENT':         ('Retry Payment',          'Entity before RETRY PYMT keyword — person name after is the account holder'),
    'MEMBERSHIP_FEE':        ('Membership Fee',         'Entity before CLUB FEES keyword — person name after is the member, not the counterparty'),
    'PERSON_SENDER':         ('Person Sender',          'Person or entity name before SENDER keyword — this is the party sending funds'),
    'PERSON_AS_BUSINESS':    ('Person as Business',     'Person name followed by BUSINESS suffix — individual operating under their own name as a business'),
    'DIRECT_PURCHASE':       ('Direct Purchase',        'Entity before PURCHASE suffix — entity name extracted, PURCHASE keyword stripped'),
    'INTERNAL_TRANSFER':     ('Internal Transfer',      'Transfer Out or Transfer In — entity extracted after the direction keyword'),
    'GOVERNMENT_PAYMENT':    ('Government Payment',     'Government agency string — person name after agency is the account holder receiving the payment'),
    'TAX_REFUND_PROCESSOR':  ('Tax Refund Processor',   'TPG Products tax refund processor string — person name after entity is the account holder receiving the refund'),
    'UNEMPLOYMENT_BENEFIT':  ('Unemployment Benefit',   'Unemployment or UI payment — agency name extracted, person name after is the account holder'),
    'TARGET_ACH_DEBIT':      ('Target ACH Debit',       'Target debit card ACH format — Target is the counterparty, person name after is the account holder'),
    'UNRECOGNISED':          ('Unrecognised',            'No pattern matched — best-effort extraction after stripping trailing noise and location suffixes'),
}

def classify_entity(counterparty, pattern):
    if not counterparty:
        return 'UNKNOWN'
    # Patterns that by definition produce an individual
    individual_patterns = {'PAYMENT_RAIL','PERSON_SENDER','PERSON_AS_BUSINESS'}
    if pattern in individual_patterns:
        return 'INDIVIDUAL'
    # Patterns that by definition produce a business/entity
    business_patterns = {
        'PAYROLL_DIRECT_DEPOSIT','PAYROLL_DEPOSIT','RETURN_TRANSACTION',
        'HEALTHME_PLATFORM','INDIAN_EAGLE','WALMART_VARIANT','SQUARE_MERCHANT',
        'NNT_MERCHANT','SY_MERCHANT','VENDOR_PAYMENT','TRANSACTION_FEE',
        'MOBILE_PAYMENT','RETRY_PAYMENT','MEMBERSHIP_FEE','GOVERNMENT_PAYMENT',
        'TAX_REFUND_PROCESSOR','UNEMPLOYMENT_BENEFIT','TARGET_ACH_DEBIT',
        'MERCHANT_WITH_CODE','STANDARD_ACH'
    }
    if pattern in business_patterns:
        return 'BUSINESS'
    # Check for business legal suffixes
    biz_suffixes = r'\b(LLC|INC|CORP|LTD|CO|PLC|LP|LLP|NA|FSB|FCU|CU|ASSOC|GROUP|SERVICES|SOLUTIONS|SYSTEMS|HOLDINGS|ENTERPRISES|PARTNERS|FOUNDATION|TRUST|BANK|FINANCIAL|CREDIT UNION|HEALTHCARE|MEDICAL|TECHNOLOGIES|TECHNOLOGY|INDUSTRIES|CONSTRUCTION|MANAGEMENT|PROPERTIES|REALTY|INSURANCE|CONSULTING|ASSOCIATES)\b'
    if re.search(biz_suffixes, counterparty.upper()):
        return 'BUSINESS'
    # Contains comma — likely LASTNAME, FIRSTNAME
    if ',' in counterparty:
        return 'INDIVIDUAL'
    # Mixed case two-word name pattern
    if re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+$', counterparty):
        return 'INDIVIDUAL'
    return 'UNKNOWN'

def check_disposition(counterparty):
    if not counterparty or counterparty == 'UNKNOWN':
        return 'REQUIRES_EXTERNAL_RESEARCH', None, None

    cp_upper = counterparty.upper().strip()

    # Exclusion — exact match on plain terms
    if cp_upper in EXCLUSION_TERMS:
        return 'EXCLUDED', None, None

    # Exclusion — regex pattern match
    for pattern in EXCLUSION_REGEX:
        if pattern.fullmatch(cp_upper) or pattern.search(cp_upper):
            return 'EXCLUDED', None, None

    # Whitelist — contains match (alias contained in counterparty or counterparty in alias)
    for alias, meta in WHITELIST_ALIASES.items():
        if alias in cp_upper or cp_upper in alias:
            return 'WHITELISTED', meta['canonical'], meta['sector']

    return 'REQUIRES_EXTERNAL_RESEARCH', None, None

# ─────────────────────────────────────────────
# EXTRACTION CORE
# ─────────────────────────────────────────────
def extract(raw: str, transaction_id=None) -> dict:
    s = raw.strip()
    result = {
        'transaction_id':       transaction_id,
        'counterparty':         None,
        'pattern_code':         None,
        'pattern_name':         None,
        'pattern_description':  None,
        'entity_type':          None,
        'disposition':          None,
        'canonical_name':       None,
        'sector':               None,
        'raw':                  raw
    }

    def ret(cp, pat):
        meta = PATTERN_META.get(pat, (pat, ''))
        # Final cleanup pass on every counterparty
        cp = strip_person_name(clean_counterparty(cp), pat) if cp and cp != 'UNKNOWN' else cp
        result['counterparty']        = cp
        result['pattern_code']        = pat
        result['pattern_name']        = meta[0]
        result['pattern_description'] = meta[1]
        result['entity_type']         = classify_entity(cp, pat)
        disp, canon, sec              = check_disposition(cp)
        result['disposition']         = disp
        result['canonical_name']      = canon
        result['sector']              = sec
        return result

    # 0a. Cashback dash-delimited format: Cashback---ID---Merchant---ID---Code
    if re.match(r'^Cashback---', s, re.IGNORECASE):
        parts = [p for p in s.split('---') if p]
        for p in parts[1:]:
            if p and not p.isdigit() and not p.lower().startswith('cashback'):
                return ret(p.strip(), 'MERCHANT_WITH_CODE')
        return ret('UNKNOWN', 'MERCHANT_WITH_CODE')

    # 0b. Credit Builder Payment — extract entity name, strip trailing colon and reference number
    m = re.match(r'^(Credit Builder Payment)\s*:\s*\d+', s, re.IGNORECASE)
    if m: return ret(m.group(1).strip(), 'STANDARD_ACH')

    # 1. Healthme
    if re.match(r'^Healthme\s+[A-Z0-9]{3,8}$', s, re.IGNORECASE):
        return ret('Healthme', 'HEALTHME_PLATFORM')

    # 2. ACH wrapping Healthme or Indian Eagle
    m = re.match(r'^ACH\s+(Credit|Debit)\s+\S+\s+(.+?)(CCD|PPD|WEB)?\s*$', s, re.IGNORECASE)
    if m:
        body = re.sub(r'\s*(CCD|PPD|WEB)\s*$', '', m.group(2)).strip()
        if re.search(r'healthme', body, re.IGNORECASE):
            return ret('Healthme', 'HEALTHME_PLATFORM')
        if re.search(r'indian eagle', body, re.IGNORECASE):
            return ret(strip_noise(body), 'INDIAN_EAGLE')
        return ret(strip_noise(body), 'ACH_TRANSACTION')

    # 3. Indian Eagle
    if re.match(r'^INDIAN EAGLE', s, re.IGNORECASE):
        m2 = re.match(r'^(INDIAN EAGLE(?:\s+PVT)?)\s+CAS-\d+', s, re.IGNORECASE)
        return ret(m2.group(1).strip() if m2 else 'INDIAN EAGLE', 'INDIAN_EAGLE')

    # 4. Telephone Transfer — no counterparty extractable
    if re.match(r'^Telephone Transfer', s, re.IGNORECASE):
        return ret('UNKNOWN', 'TELEPHONE_TRANSFER')

    # 5. IMAD/OMAD wire
    if re.match(r'^IMAD\s*#', s, re.IGNORECASE):
        m2 = re.search(r'(?:Incoming|Outgoing)\s+(?:Domestic\s+)?Wire\S*\s+\d+\s+(.+?)$', s, re.IGNORECASE)
        if m2:
            entity = m2.group(1).strip()
            # Wire strings end with: ENTITY_NAME  ROUTING_NUMBER BANKCODE
            # Strip trailing routing number (9 digits) and short bank codes
            # Strip routing number (9 digits) + bank code in one pass
            entity = re.sub(r'\s+\d{9,}\s+\S+\s*$', '', entity).strip()
            entity = re.sub(r'\s+\d{6,}\s*$', '', entity).strip()
            entity = strip_noise(entity)
        else:
            entity = 'UNKNOWN'
        return ret(entity, 'WIRE_TRANSFER')

    # 6. Helix Return
    m = re.match(r'^Return of a (?:Deposit from|Withdrawal to)\s+(.+?)\*\d+', s, re.IGNORECASE)
    if m: return ret(m.group(1).strip(), 'RETURN_TRANSACTION')

    # 7. Helix Transfer (clean bank-to-bank only)
    m = re.match(r'^Transfer from\s+(.+?)\s+to\s+', s, re.IGNORECASE)
    if m:
        entity = m.group(1).strip()
        # If it looks like an account number or internal code, don't return it
        if re.match(r'^(DDA|XXX|\d)', entity, re.IGNORECASE):
            return ret('UNKNOWN', 'ACCOUNT_TRANSFER')
        return ret(strip_noise(entity), 'ACCOUNT_TRANSFER')

    # 8. Transfer Out/In — don't strip noise, entity name is everything after Out/In
    m = re.match(r'^Transfer\s+(Out|In)\s+(.+)$', s, re.IGNORECASE)
    if m: return ret(m.group(2).strip(), 'INTERNAL_TRANSFER')

    # 9. WAL WAL-MART
    if re.match(r'^WAL\s+WAL-MART', s, re.IGNORECASE):
        m2 = re.match(r'^(WAL\s+WAL-MART(?:\s+\w+)?)\s+\d+,', s, re.IGNORECASE)
        return ret(m2.group(1).strip() if m2 else 'WAL WAL-MART', 'WALMART_VARIANT')

    # 10. SY# merchant
    m = re.match(r'^SY\d\s+(.+?)\s+[A-Z]{2,3}\d{4,6},', s)
    if m: return ret(m.group(1).strip(), 'SY_MERCHANT')

    # 11. NNT merchant — strip store code like S000243, #12000984
    m = re.match(r'^NNT\s+(.+?)\s{2,}\d+,', s)
    if not m:
        m = re.match(r'^NST\s+(.+?)(?:,|\s*$)', s)
    if m:
        merchant = re.sub(r'\s*[#S]\d{3,}\s*$', '', m.group(1).strip()).strip()
        merchant = re.sub(r'\s+\d{4,}\s*$', '', merchant).strip()
        return ret(merchant, 'NNT_MERCHANT')

    # 11b. SP (ShopPay/Stripe) merchant — "SP MR LOCK, 178-64721235, DE 19709 US"
    m = re.match(r'^SP\s+(.+?)(?:,|$)', s)
    if m: return ret(strip_location(strip_noise(m.group(1).strip())), 'MERCHANT_WITH_CODE')

    # 12. SQ merchant
    m = re.match(r'^SQ \*(.+?)(?:,|$)', s)
    if m: return ret(strip_location(m.group(1).strip()), 'SQUARE_MERCHANT')

    # 13. TPG Products / Tax Products / SBTPG
    if re.match(r'^(TPG PRODUCTS|TAX PRODUCTS)', s, re.IGNORECASE):
        return ret('TPG PRODUCTS SBTPG LLC', 'TAX_REFUND_PROCESSOR')

    # 13a. INTUIT TURBOTAX — extract just "INTUIT TURBOTAX"
    if re.match(r'^INTUIT\s+TURBOTAX', s, re.IGNORECASE):
        return ret('INTUIT TURBOTAX', 'TAX_REFUND_PROCESSOR')

    # 13b. WM SUPERCENTER / Walmart POS
    if re.match(r'^WM\s+SUPERCENTER', s, re.IGNORECASE):
        return ret('WALMART', 'WALMART_VARIANT')

    # 14. TARGET DEBIT CRD ACH TRAN
    if re.match(r'^TARGET DEBIT CRD ACH TRAN', s, re.IGNORECASE):
        return ret('TARGET', 'TARGET_ACH_DEBIT')

    # 15. UI / Unemployment
    if re.match(r'^(TN UI|CTDOL UNEMP)', s, re.IGNORECASE):
        m2 = re.match(r'^(.+?)\s+(TNUIDD|UNEMP COMP|BENEFITS UI)\b', s, re.IGNORECASE)
        if m2: return ret(strip_noise(m2.group(1).strip()), 'UNEMPLOYMENT_BENEFIT')

    # 16. Direct Dep
    m = re.match(r'^(.+?)\s+DIR(?:ECT)?\s+DEP\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'PAYROLL_DIRECT_DEPOSIT')

    # 17. Payroll — also matches PPDPAYROLL, SCHOOL PR, PYRL
    m = re.match(r'^(.+?)\s+(?:OFF ?CYCLE\s+)?(?:PPD)?PAYROLL\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'PAYROLL_DEPOSIT')
    m = re.match(r'^(.+?)\s+(?:SCHOOL\s+PR|PYRL)\d*\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'PAYROLL_DEPOSIT')

    # 18. Retry Payment
    m = re.match(r'^(.+?)\s+RETRY PYMT\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'RETRY_PAYMENT')

    # 19. Mobile Payment
    m = re.match(r'^(.+?)\s+MOBILE PMT\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'MOBILE_PAYMENT')

    # 20. Club Fees
    m = re.match(r'^(.+?)\s+CLUB FEES\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'MEMBERSHIP_FEE')

    # 21. Vendor Payment
    m = re.match(r'^(.+?)\s+VENDOR PMT\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'VENDOR_PAYMENT')

    # 22. Transaction Fee
    m = re.match(r'^(.+?)\s+\d+\s+TRAN FEE\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'TRANSACTION_FEE')

    # 23. Sender
    m = re.match(r'^(.+?)\s+SENDER\s*$', s, re.IGNORECASE)
    if m: return ret(m.group(1).strip(), 'PERSON_SENDER')

    # 24. Business
    m = re.match(r'^(.+?)\s+BUSINESS\s*$', s, re.IGNORECASE)
    if m: return ret(m.group(1).strip(), 'PERSON_AS_BUSINESS')

    # 25. Purchase
    m = re.match(r'^(.+?)\s+(?:#\S+\s+)?PURCHASE\s*$', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'DIRECT_PURCHASE')

    # 26. Government
    for gov in GOV_PATTERNS:
        if re.match(gov, s, re.IGNORECASE):
            agency = re.split(r'\s+[A-Z]*REFUNDS?\b|\s+TAXREFUNDS?\b|\s+UNEMP\b|\s+DE DOR\b|\s+BENEFITS\b|\s+CHILDCTC\b|\s+TAX REF(?:UND)?\b|\s+WITHDRAWAL\b|\s+[A-Z]{2,}STTAXRFD\b|\s+[A-Z]{2,}TAXPYMT\b|\s+USATAXPYMT\b', s, flags=re.IGNORECASE)[0]
            return ret(strip_noise(agency.strip()), 'GOVERNMENT_PAYMENT')

    # 27. POS / DDA
    if re.match(r'^POS\s+(Debit|Pre-Authorized|Recurring)', s, re.IGNORECASE):
        m2 = re.search(r'\d{8}([A-Za-z][A-Za-z0-9\s\'\-&#*]+?)(?:\s+\d|\s+[A-Z]{2}\s|\s+C#|\s*$)', s)
        if m2:
            merchant = re.sub(r'\s+(DG|DF|FA|WV|TX|CA|PA|NC|FL|GA|OH|IN|MO|IL|TN|NY|VA|MD|SC)\s*$', '', m2.group(1).strip()).strip()
            merchant = strip_noise(merchant)
            if len(merchant) > 2:
                return ret(merchant, 'POS_CARD_TRANSACTION')
        return ret('UNKNOWN', 'POS_CARD_TRANSACTION')

    # 28. Platform * Person or Merchant *
    if '*' in s:
        parts = s.split('*', 1)
        entity_before = parts[0].strip()
        eu = entity_before.upper()
        after = parts[1].strip() if len(parts) > 1 else ''
        is_rail     = any(r in eu for r in PAYMENT_RAILS)
        is_merchant = any(r in eu for r in KNOWN_MERCHANTS)
        if is_rail and not is_merchant:
            after_clean = re.sub(r',.*$', '', after).strip()
            after_clean = strip_noise(after_clean).split('*')[0].strip()
            # DK*DRAFTKINGS variant codes — normalize to DRAFTKINGS
            if eu.startswith('DK') and re.match(r'^DRAFTKINGS', after_clean, re.IGNORECASE):
                return ret('DRAFTKINGS', 'PAYMENT_RAIL')
            return ret(after_clean if after_clean else entity_before, 'PAYMENT_RAIL')
        else:
            return ret(strip_noise(entity_before), 'MERCHANT_WITH_CODE')

    # 29. NACHA standard suffixes
    m = re.match(r'^(.+?)\s+(FUND PMTS|PAYABLES|PMT REFUND|ACCTVERIFY)\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'STANDARD_ACH')
    m = re.match(r'^(.+?)\s+(?:EFT|ACH)\d*\s*$', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'STANDARD_ACH')

    # 29b. Credit card / web payments: "COMPANY WEB PYMT PERSON REF"
    m = re.match(r'^(.+?)\s+(WEB PYMT|WEB PMT|EPAY|INTERNET|INS PREM|LOAN PYMNT?)\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'STANDARD_ACH')

    # 29c. BILT/rent payments, fintech patterns: "BILT PAYMENT BILTRENT Name Code"
    m = re.match(r'^(.+?)\s+(BILTRENT|SMARTPAY|AUTOPAY)\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'STANDARD_ACH')

    # 30. Fallback
    cp = strip_location(s)
    cp = strip_noise(cp)
    return ret(cp, 'UNRECOGNISED')


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/extract', methods=['POST'])
def extract_route():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON body'}), 400
    if 'description' in data:
        return jsonify(extract(data['description'], data.get('transaction_id')))
    if 'transactions' in data:
        results = []
        for item in data['transactions']:
            if isinstance(item, str):
                results.append(extract(item))
            elif isinstance(item, dict):
                results.append(extract(item.get('description',''), item.get('transaction_id')))
        return jsonify({'results': results, 'count': len(results)})
    return jsonify({'error': "Provide 'description' or 'transactions'"}), 400

@app.route('/upload', methods=['POST'])
def upload_route():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    f = request.files['file']
    fname = f.filename.lower()

    # Read uploaded file
    try:
        if fname.endswith('.xlsx') or fname.endswith('.xls'):
            df = pd.read_excel(f)
        elif fname.endswith('.csv'):
            df = pd.read_csv(f)
        else:
            return jsonify({'error': 'Unsupported format. Please upload .xlsx or .csv'}), 400
    except Exception as e:
        return jsonify({'error': f'Could not read file: {str(e)}'}), 400

    df.columns = [c.strip() for c in df.columns]

    # Find the description column (case-insensitive)
    desc_col = None
    tid_col = None
    for c in df.columns:
        cl = c.lower()
        if cl in ('description', 'transaction_description', 'trans_description', 'txn_description'):
            desc_col = c
        if cl in ('transaction_id', 'trans_id', 'txn_id', 'id'):
            tid_col = c
    if desc_col is None:
        return jsonify({'error': f"No 'description' column found. Columns present: {list(df.columns)}"}), 400

    # Process each row
    enrichment_cols = ['counterparty', 'pattern_name', 'entity_type', 'disposition', 'canonical_name', 'sector']
    rows = []
    for _, row in df.iterrows():
        desc = str(row[desc_col]).strip() if pd.notna(row[desc_col]) else ''
        tid = str(row[tid_col]).strip() if tid_col and pd.notna(row.get(tid_col)) else None
        if desc and desc != 'nan':
            r = extract(desc, tid)
            rows.append({col: r.get(col, '') for col in enrichment_cols})
        else:
            rows.append({col: '' for col in enrichment_cols})

    enriched = pd.DataFrame(rows)
    for col in enrichment_cols:
        df[col] = enriched[col].values

    # Write to xlsx in memory
    output = io.BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='counterparty_enriched.xlsx'
    )

@app.route('/sample')
def sample_download():
    return send_from_directory('static', 'sample_transactions.xlsx', as_attachment=True)

@app.route('/health')
def health():
    return jsonify({
        'status':            'ok',
        'version':           '3.0.0',
        'exclusion_terms':   len(EXCLUSION_TERMS),
        'whitelist_aliases': len(WHITELIST_ALIASES)
    })

@app.route('/stats')
def stats():
    return jsonify({
        'patterns':          len(PATTERN_META),
        'exclusion_terms':   len(EXCLUSION_TERMS),
        'whitelist_aliases': len(WHITELIST_ALIASES),
        'exclusion_regex_patterns': len(EXCLUSION_REGEX),
        'list_versions':     {
            'exclusion_list': os.environ.get('EXCLUSION_LIST_PATH','data/Counterparty_Search_Exclusion_List_v9.xlsx'),
            'whitelist':      os.environ.get('WHITELIST_PATH','data/counterparty_whitelist_v8.xlsx')
        }
    })

os.makedirs('static', exist_ok=True)
os.makedirs('data', exist_ok=True)
load_lists()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
