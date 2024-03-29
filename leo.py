import pandas as pd
import os
from datetime import datetime

# Ścieżki do plików, w których będą przechowywane ostatnie ID
last_ids = {
    'make': 'last_make_id.txt',
    'model': 'last_model_id.txt',
    'year': 'last_year_id.txt',
    'product': 'last_product_id.txt'
}

def append_to_csv(df, output_directory, filename):
    """Dodaje nowe dane do istniejącego pliku CSV lub tworzy nowy, jeśli plik nie istnieje."""
    file_path = os.path.join(output_directory, filename)
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path)
        updated_df = pd.concat([existing_df, df], ignore_index=True)
        updated_df.to_csv(file_path, index=False)
    else:
        df.to_csv(file_path, index=False)

def get_next_id(entity):
    """
    Funkcja zwraca następne ID dla danej encji i inkrementuje licznik.
    """
    last_id_file = last_ids[entity]
    if os.path.exists(last_id_file):
        with open(last_id_file, 'r') as file:
            last_id = int(file.read().strip())
    else:
        last_id = 0
    next_id = last_id + 1
    with open(last_id_file, 'w') as file:
        file.write(str(next_id))
    return next_id

def generate_csv_files(source_df, output_directory):
    # Unikalne wartości dla 'Make', 'Model', 'Year'
    unique_makes = source_df['Make'].unique()
    unique_models = source_df['Model'].unique()
    unique_years = source_df['Year'].unique()

    # Tabela 'Make'
    make_df = pd.DataFrame({
        'id_leopartsfilter_make': range(1, len(unique_makes) + 1),
        'name': unique_makes,
        'position': 0,
        'active': 1,
        'date_add': datetime.now().strftime('%Y-%m-%d')
    })

    # Tabela 'Make Lang'
    make_lang_df = pd.DataFrame({
        'id_leopartsfilter_make': make_df['id_leopartsfilter_make'].repeat(2),
        'name': make_df['name'].repeat(2),
        'id_lang': [1, 3] * len(unique_makes)
    })

    # Tabela 'Make Shop'
    make_shop_df = pd.DataFrame({
        'id_leopartsfilter_make': make_df['id_leopartsfilter_make'],
        'id_shop': 1
    })

    # Tworzenie słownika mapowania nazw modeli na id_leopartsfilter_make
    make_to_id = dict(zip(make_df['name'], make_df['id_leopartsfilter_make']))
    model_to_make_id = {model: make_to_id[source_df[source_df['Model'] == model]['Make'].iloc[0]] for model in unique_models}

    # Tabela 'Model' z poprawką
    model_df = pd.DataFrame({
        'id_leopartsfilter_model': range(1, len(unique_models) + 1),
        'id_leopartsfilter_make': [model_to_make_id[model] for model in unique_models],
        'name': unique_models,
        'position': 0,
        'active': 1,
        'date_add': datetime.now().strftime('%Y-%m-%d')
    })


    # Tabela 'Model Lang'
    model_lang_df = pd.DataFrame({
        'id_leopartsfilter_model': model_df['id_leopartsfilter_model'].repeat(2),
        'name': model_df['name'].repeat(2),
        'id_lang': [1, 3] * len(unique_models)
    })

    # Tabela 'Model Shop'
    model_shop_df = pd.DataFrame({
        'id_leopartsfilter_model': model_df['id_leopartsfilter_model'],
        'id_shop': 1
    })

        # Przygotowanie danych do tabeli 'Year'
    year_data = source_df[['Year', 'Make', 'Model']].drop_duplicates()

    # Utworzenie mapowania dla marek i modeli
    make_to_id = dict(zip(make_df['name'], make_df['id_leopartsfilter_make']))
    model_to_id = {model: model_df[model_df['name'] == model]['id_leopartsfilter_model'].iloc[0] for model in unique_models}

    # Dodanie id dla marek i modeli do year_data
    year_data['id_leopartsfilter_make'] = year_data['Make'].map(make_to_id)
    year_data['id_leopartsfilter_model'] = year_data['Model'].map(model_to_id)

    # Utworzenie końcowej tabeli 'Year' z prawidłowym mapowaniem
    year_df = year_data[['Year', 'id_leopartsfilter_make', 'id_leopartsfilter_model']].drop_duplicates()
    year_df = year_df.rename(columns={'Year': 'name'})
    year_df['id_leopartsfilter_year'] = range(1, len(year_df) + 1)
    year_df['position'] = 0
    year_df['active'] = 1
    year_df['date_add'] = datetime.now().strftime('%Y-%m-%d')


    # Tabela 'Year Lang'
    year_lang_df = pd.DataFrame({
    'id_leopartsfilter_year': year_df['id_leopartsfilter_year'].repeat(2).reset_index(drop=True),
    'name': year_df['name'].repeat(2).reset_index(drop=True),
    'id_lang': [1, 3] * len(year_df)
})

    # Tabela 'Year Shop'
    year_shop_df = pd.DataFrame({
        'id_leopartsfilter_year': year_df['id_leopartsfilter_year'],
        'id_shop': 1
    })

    # Tabela 'Product'
    product_df = pd.DataFrame({
        'id_product': source_df['Product ID'],
        'id_leopartsfilter_make': [make_df[make_df['name'] == make]['id_leopartsfilter_make'].iloc[0] for make in source_df['Make']],
        'id_leopartsfilter_model': [model_df[model_df['name'] == model]['id_leopartsfilter_model'].iloc[0] for model in source_df['Model']],
        'id_leopartsfilter_year': [year_df[year_df['name'] == year]['id_leopartsfilter_year'].iloc[0] for year in source_df['Year']],
        'id_leopartsfilter_device': 0,
        'id_leopartsfilter_level5': 0
    })

    # Pobieranie następnego ID dla każdej encji
    next_make_id = get_next_id('make')
    next_model_id = get_next_id('model')
    next_year_id = get_next_id('year')
    next_product_id = get_next_id('product')

    # Kontynuowanie numeracji ID dla każdej z tabel
    make_df['id_leopartsfilter_make'] += next_make_id
    model_df['id_leopartsfilter_model'] += next_model_id
    year_df['id_leopartsfilter_year'] += next_year_id
    product_df['id_product'] += next_product_id

    # Zapisywanie DataFrame'ów do plików CSV z użyciem append_to_csv
    append_to_csv(make_df, output_directory, 'ps_leopartsfilter_make.csv')
    append_to_csv(make_lang_df, output_directory, 'ps_leopartsfilter_make_lang.csv')
    append_to_csv(make_shop_df, output_directory, 'ps_leopartsfilter_make_shop.csv')
    append_to_csv(model_df, output_directory, 'ps_leopartsfilter_model.csv')
    append_to_csv(model_lang_df, output_directory, 'ps_leopartsfilter_model_lang.csv')
    append_to_csv(model_shop_df, output_directory, 'ps_leopartsfilter_model_shop.csv')
    append_to_csv(year_df, output_directory, 'ps_leopartsfilter_year.csv')
    append_to_csv(year_lang_df, output_directory, 'ps_leopartsfilter_year_lang.csv')
    append_to_csv(year_shop_df, output_directory, 'ps_leopartsfilter_year_shop.csv')
    append_to_csv(product_df, output_directory, 'ps_leopartsfilter_product.csv')

def scan_directories_and_generate_csv(input_directories, output_directory):
    for input_directory in input_directories:
        for filename in os.listdir(input_directory):
            if filename.endswith('.csv'):
                source_file = os.path.join(input_directory, filename)
                source_df = pd.read_csv(source_file)
                generate_csv_files(source_df, output_directory)

# Lista ścieżek do folderów źródłowych
input_directories = ['x']  

# Ścieżka do folderu wynikowego
output_directory = 'x'

# Wywołanie funkcji skanującej foldery
scan_directories_and_generate_csv(input_directories, output_directory)
