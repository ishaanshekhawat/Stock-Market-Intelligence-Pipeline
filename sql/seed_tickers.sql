INSERT INTO raw.dim_tickers (symbol, company_name, exchange, sector, industry) VALUES
    ('AAPL',  'Apple Inc.',                    'NASDAQ', 'Technology',            'Consumer Electronics'),
    ('MSFT',  'Microsoft Corporation',          'NASDAQ', 'Technology',            'Software'),
    ('GOOGL', 'Alphabet Inc.',                  'NASDAQ', 'Technology',            'Internet Services'),
    ('AMZN',  'Amazon.com Inc.',                'NASDAQ', 'Consumer Discretionary','E-Commerce'),
    ('META',  'Meta Platforms Inc.',            'NASDAQ', 'Technology',            'Social Media'),
    ('NVDA',  'NVIDIA Corporation',             'NASDAQ', 'Technology',            'Semiconductors'),
    ('JPM',   'JPMorgan Chase & Co.',           'NYSE',   'Financials',            'Banking')
ON CONFLICT (symbol) DO NOTHING;
