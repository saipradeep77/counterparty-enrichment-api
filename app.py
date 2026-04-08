from flask import Flask, request, jsonify, send_from_directory
import re, os, pandas as pd

app = Flask(__name__, static_folder='static')

# ─────────────────────────────────────────────
# LISTS — loaded once at startup
# ─────────────────────────────────────────────
EXCLUSION_TERMS = set()      # plain string exact match (uppercased)
EXCLUSION_REGEX  = []        # compiled regex patterns from exclusion list
WHITELIST_ALIASES = {}       # alias_upper -> {canonical, sector}

def load_lists():
    global EXCLUSION_TERMS, EXCLUSION_REGEX, WHITELIST_ALIASES
    excl_path = os.environ.get('EXCLUSION_LIST_PATH', 'data/Counterparty_Search_Exclusion_List_v9.xlsx')
    wl_path   = os.environ.get('WHITELIST_PATH',      'data/counterparty_whitelist_v8.xlsx')

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
    text = re.sub(r',\s*[\w\s]+,\s*[A-Z]{2}\s*\d{0,5}\s*(US|MX)?\s*$', '', text).strip()
    text = re.sub(r',?\s*\d{3}[-.\s]\d{3}[-.\s]\d{4}', '', text).strip()
    text = re.sub(r'\s+[A-Z]{2}\s+\d{5}\s+(US|MX)\s*$', '', text).strip()
    return text.strip().rstrip(',').strip()

def strip_noise(text):
    text = re.sub(r'\s+[A-Z0-9]{8,}\s*$', '', text).strip()
    text = re.sub(r'\s+\d{6,}\s*$', '', text).strip()
    text = re.sub(r'\s+[A-Z]{2,4}-\d+\s*$', '', text).strip()
    text = re.sub(r'\s+x+\d+\s*(PRENOTE)?\s*$', '', text, flags=re.IGNORECASE).strip()
    text = re.sub(r'\s+CCD\s*$', '', text).strip()
    return text.strip()

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
    r'^STATE OF\s+\w+',r'^IRS\b',r'^Division of',r'^US DEPT',r'^DEPT OF',
    r'^CTDOL\b',r'^TN UI\b',
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

    # 0. Credit Builder Payment — extract entity name, strip trailing colon and reference number
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
        entity = strip_noise(m2.group(1).strip()) if m2 else 'UNKNOWN'
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

    # 11. NNT merchant
    m = re.match(r'^NNT\s+(.+?)\s{2,}\d+,', s)
    if m: return ret(m.group(1).strip(), 'NNT_MERCHANT')

    # 12. SQ merchant
    m = re.match(r'^SQ \*(.+?)(?:,|$)', s)
    if m: return ret(strip_location(m.group(1).strip()), 'SQUARE_MERCHANT')

    # 13. TPG Products
    if re.match(r'^TPG PRODUCTS', s, re.IGNORECASE):
        return ret('TPG PRODUCTS SBTPG LLC', 'TAX_REFUND_PROCESSOR')

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

    # 17. Payroll
    m = re.match(r'^(.+?)\s+(?:OFF ?CYCLE\s+)?PAYROLL\b', s, re.IGNORECASE)
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
            agency = re.split(r'\s+[A-Z]+REFUNDS?\b|\s+TAXREFUNDS?\b|\s+UNEMP\b|\s+DE DOR\b|\s+BENEFITS\b|\s+CHILDCTC\b|\s+TAX REF\b', s, flags=re.IGNORECASE)[0]
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
            return ret(after_clean if after_clean else entity_before, 'PAYMENT_RAIL')
        else:
            return ret(strip_noise(entity_before), 'MERCHANT_WITH_CODE')

    # 29. NACHA standard suffixes
    m = re.match(r'^(.+?)\s+(FUND PMTS|PAYABLES|PMT REFUND|ACCTVERIFY)\b', s, re.IGNORECASE)
    if m: return ret(strip_noise(m.group(1).strip()), 'STANDARD_ACH')
    m = re.match(r'^(.+?)\s+(?:EFT|ACH)\d*\s*$', s, re.IGNORECASE)
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

if __name__ == '__main__':
    os.makedirs('static', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    load_lists()
    app.run(debug=True, port=5000)
