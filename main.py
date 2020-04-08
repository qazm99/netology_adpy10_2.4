from pymongo import MongoClient
import csv
from datetime import datetime as dt


def read_data(csv_file, db):
    try:
        with open(csv_file, encoding='utf8') as csvfile:
            # прочитать файл с данными и записать в коллекцию
            reader = list(csv.DictReader(csvfile, delimiter=','))
            for ticket in reader:
                ticket['Цена'] = int(ticket['Цена'])
                ticket['Дата'] = dt.strptime(ticket['Дата'] +'.2020', '%d.%m.%Y')
            result = db['tickets'].insert_many(reader)
            return result
    except Exception as e:
        print(f'Не удалось загрузить данные с файла {csv_file} в БД {db} по причине: {e}')


def find_cheapest(db):
    if db['tickets'].count_documents({}):
        return db['tickets'].find().sort('Цена')


def find_by_name(name, db):
    if db['tickets'].count_documents({'Исполнитель': {"$regex": f"[.]*{name}[.]*", "$options": 'i'}}):
        return db['tickets'].find({'Исполнитель': {"$regex": f"[.]*{name}[.]*", "$options": 'i'}}).sort('Цена')


def show_collection(db,name_collection):
    if db[name_collection].count_documents({}):
        return db[name_collection].find()


if __name__ == '__main__':
    while True:
        client = MongoClient()
        netology_db = client['netology']
        print('Попробуем подгрузить билеты из файла в БД MongoDB')
        if input('Отчистить таблицу с билетами?(y):').upper() == 'Y':
            if netology_db['tickets'].find():
                netology_db['tickets'].drop()

        file_name_in = input('Введите имя файла для загрузки в таблицу билетов?:')
        if not file_name_in:
            file_name_in = 'artists.csv'
            print(f'Раз вы не определились с фалом - то будет {file_name_in}')
        if read_data(file_name_in, netology_db):
            print('Данные из файла прочитаны и добавлены в БД')

        if input('Отобразить данные коллекции tickets?(y):').upper() == 'Y':
            if show_collection(netology_db, 'tickets'):
                for num, ticket in enumerate(netology_db['tickets'].find({})):
                    print(f'{num}-{ticket}')
            else:
                print('В этой коллекции нет данных')

        if input('Попробуем упорядочить билеты по цене. Показать что получилось?(y):').upper() == 'Y':
            cheapest_tickets = find_cheapest(netology_db)
            if cheapest_tickets:
                for num, ticket in enumerate(cheapest_tickets):
                    print(num, ' ', ticket)
            else:
                print('В этой коллекции нет данных')

        if input('Поищем билеты по исполнителю?(y):').upper() == 'Y':
            find_artist = input('Введите имя исполнителя для поиска или оставьте поле пустым'
                                ' для поиска по имени ДжаZ:')
            if not find_artist:
                find_artist = 'ДжаZ'
            all_tickets_by_name = find_by_name(find_artist, netology_db)
            if all_tickets_by_name:
                for row in all_tickets_by_name:
                    print(row)
            else:
                print(f'Не найдены мероприятия с участием {find_artist}')

        if input('Попробуем снова?(y):').upper() != 'Y':
            print('Хорошего дня!')
            break
