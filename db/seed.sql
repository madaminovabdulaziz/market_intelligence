-- Seed the 6 known rating categories
INSERT INTO rating_categories (code, name_uz, name_ru, display_order) VALUES
    ('qualified_specialists',  'Malakali mutaxassislar',       'Квалифицированные специалисты', 1),
    ('financial_performance',  'Moliyaviy ko''rsatkichlar',    'Финансовые показатели',         2),
    ('quality_of_work',        'Bajarilgan ishlar sifati',     'Качество выполненных работ',    3),
    ('work_experience',        'Ish tajribasi',                'Опыт работы',                   4),
    ('technical_base',         'Texnik imkoniyatlar/baza',     'Техническая база',              5),
    ('competitiveness',        'Raqobatbardoshlik',            'Конкурентоспособность',         6)
ON CONFLICT (code) DO NOTHING;
