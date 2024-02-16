import random
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Column, select, MetaData, VARCHAR, Integer, create_engine, UniqueConstraint, ForeignKey
from sqlalchemy.orm import relationship, registry, Session

#  Create the postgresql database engine of the database "orm_bookkeeping".
engine = create_engine("postgresql://postgres:YOUR_PASSWORD@localhost/orm_bookkeeping")
mapper_registry = registry()
Base = mapper_registry.generate_base()


# metadata = MetaData()
# metadata.reflect(engine)
# metadata.drop_all(engine)
# metadata.clear()


class Books(Base):  # Create Books table
    __tablename__ = 'books'
    id = Column(Integer, unique=True, primary_key=True, nullable=False)
    title = Column(VARCHAR(150), nullable=False)
    number_of_pages = Column(Integer, nullable=False)
    authors = relationship('Author_books', back_populates='books')
    __table_args__ = (UniqueConstraint(title, number_of_pages, name='unique_title_page_num'),)

    def __repr__(self):
        return "<Books(id = '{0}', title = '{1}', number_of_pages = '{2}')>".format(self.id, self.title,
                                                                                    self.number_of_pages)


class Authors(Base):  # Create Authors table
    __tablename__ = 'authors'
    id = Column(Integer, nullable=False, primary_key=True, unique=True)
    first_name = Column(VARCHAR(50), nullable=False)
    last_name = Column(VARCHAR(50), nullable=False)
    books = relationship('Author_books', back_populates='authors')
    __table_args__ = (UniqueConstraint(first_name, last_name, name='unique_first_last_name'),)

    def __repr__(self):
        return "<Authors(id = '{0}', first_name = '{1}, last_name = '{2}'')>".format(self.id, self.first_name,
                                                                                     self.last_name)


class Author_books(Base):  # Create Author_books table
    __tablename__ = 'author_books'
    id = Column(Integer, nullable=False, primary_key=True, autoincrement=True, unique=True)
    author_id = Column(Integer, ForeignKey(column=Authors.id, ondelete='CASCADE'), nullable=False)
    book_id = Column(Integer, ForeignKey(column=Books.id, ondelete='CASCADE'), nullable=False, unique=True)
    __table_args__ = (UniqueConstraint(author_id, book_id, name='unique_author_id_book_id'),)
    books = relationship('Books', back_populates='authors')
    authors = relationship('Authors', back_populates='books')

    def __repr__(self):
        return "<Author_books(id = '{0}', author_id = '{1}', book_id = '{2}')>".format(self.id, self.author_id,
                                                                                       self.book_id)


# Effects the CREATE operations above.
Base.metadata.create_all(engine)

data = [{'book_id': 1, 'title': 'The Mystery', 'number_of_pages': 300, 'author_id': 1, 'first_name': 'John',
         'last_name': 'Doe'},
        {'book_id': 2, 'title': 'Journey to the Stars', 'number_of_pages': 250, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 3, 'title': 'Beyond the Horizon', 'number_of_pages': 400, 'author_id': 3, 'first_name': 'Michael',
         'last_name': 'Johnson'},
        {'book_id': 4, 'title': 'Secrets Unveiled', 'number_of_pages': 320, 'author_id': 4, 'first_name': 'Sarah',
         'last_name': 'Williams'},
        {'book_id': 5, 'title': 'Echoes of Time', 'number_of_pages': 280, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {'book_id': 6, 'title': 'Whispers in the Wind', 'number_of_pages': 200, 'author_id': 6, 'first_name': 'Emily',
         'last_name': 'Taylor'},
        {'book_id': 7, 'title': 'The Silent Observer', 'number_of_pages': 350, 'author_id': 7,
         'first_name': 'Christopher', 'last_name': 'Brown'},
        {'book_id': 8, 'title': 'Enchanted Gardens', 'number_of_pages': 180, 'author_id': 8, 'first_name': 'Olivia',
         'last_name': 'Anderson'},
        {'book_id': 9, 'title': 'Shattered Dreams', 'number_of_pages': 300, 'author_id': 9, 'first_name': 'Daniel',
         'last_name': 'Wilson'},
        {'book_id': 10, 'title': 'Lost in Translation', 'number_of_pages': 240, 'author_id': 10,
         'first_name': 'Sophia', 'last_name': 'Miller'},
        {'book_id': 11, 'title': 'Midnight Serenade', 'number_of_pages': 280, 'author_id': 1, 'first_name': 'John',
         'last_name': 'Doe'},
        {'book_id': 12, 'title': 'Captivating Moments', 'number_of_pages': 320, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 13, 'title': 'Shadows of the Past', 'number_of_pages': 400, 'author_id': 3,
         'first_name': 'Michael', 'last_name': 'Johnson'},
        {'book_id': 14, 'title': 'The Forgotten Realm', 'number_of_pages': 260, 'author_id': 4, 'first_name': 'Sarah',
         'last_name': 'Williams'},
        {'book_id': 15, 'title': 'Symphony of Shadows', 'number_of_pages': 300, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {'book_id': 16, 'title': "A Garden's Tale", 'number_of_pages': 220, 'author_id': 6, 'first_name': 'Emily',
         'last_name': 'Taylor'},
        {'book_id': 17, 'title': 'The Art of Silence', 'number_of_pages': 380, 'author_id': 7,
         'first_name': 'Christopher', 'last_name': 'Brown'},
        {'book_id': 18, 'title': 'Colors of the Sky', 'number_of_pages': 200, 'author_id': 8, 'first_name': 'Olivia',
         'last_name': 'Anderson'},
        {'book_id': 19, 'title': 'Broken Reflections', 'number_of_pages': 320, 'author_id': 9, 'first_name': 'Daniel',
         'last_name': 'Wilson'},
        {'book_id': 20, 'title': 'Echoes of Eternity', 'number_of_pages': 240, 'author_id': 10, 'first_name': 'Sophia',
         'last_name': 'Miller'},
        {'book_id': 21, 'title': 'The Hidden Truth', 'number_of_pages': 300, 'author_id': 1, 'first_name': 'John',
         'last_name': 'Doe'},
        {'book_id': 22, 'title': 'Serendipity', 'number_of_pages': 250, 'author_id': 2, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 23, 'title': "The Alchemist's Legacy", 'number_of_pages': 400, 'author_id': 3,
         'first_name': 'Michael', 'last_name': 'Johnson'},
        {'book_id': 24, 'title': 'Veil of Illusions', 'number_of_pages': 320, 'author_id': 4, 'first_name': 'Sarah',
         'last_name': 'Williams'},
        {'book_id': 25, 'title': 'Eternal Odyssey', 'number_of_pages': 280, 'author_id': 5, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {'book_id': 26, 'title': 'Echoes of Eternity', 'number_of_pages': 240, 'author_id': 10, 'first_name': 'Sophia',
         'last_name': 'Miller'},
        {'book_id': 27, 'title': 'The Hidden Truth', 'number_of_pages': 300, 'author_id': 41, 'first_name': 'Michael',
         'last_name': 'Johnson'},
        {'book_id': 28, 'title': 'Serendipity', 'number_of_pages': 250, 'author_id': 42, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 29, 'title': "The Alchemist's Legacy", 'number_of_pages': 400, 'author_id': 43,
         'first_name': 'Michael', 'last_name': 'Johnson'},
        {'book_id': 30, 'title': 'Veil of Illusions', 'number_of_pages': 320, 'author_id': 44, 'first_name': 'Jane',
         'last_name': 'Smith'},
        {'book_id': 31, 'title': 'Eternal Odyssey', 'number_of_pages': 280, 'author_id': 45, 'first_name': 'Robert',
         'last_name': 'Davis'},
        {"book_id": 1, "title": "The Alchemist", "number_of_pages": 208, "author_id": 11, "first_name": "Paulo",
         "last_name": "Coelho"},
        {"book_id": 32, "title": "To Kill a Mockingbird", "number_of_pages": 336, "author_id": 12,
         "first_name": "Harper", "last_name": "Lee"},
        {"book_id": 33, "title": "The Great Gatsby", "number_of_pages": 180, "author_id": 13, "first_name": "F. Scott",
         "last_name": "Fitzgerald"},
        {"book_id": 34, "title": "Pride and Prejudice", "number_of_pages": 432, "author_id": 14, "first_name": "Jane",
         "last_name": "Austen"},
        {"book_id": 35, "title": "One Hundred Years of Solitude", "number_of_pages": 417, "author_id": 15,
         "first_name": "Gabriel", "last_name": "García Márquez"},
        {"book_id": 36, "title": "The Lord of the Rings", "number_of_pages": 1178, "author_id": 16,
         "first_name": "J.R.R.", "last_name": "Tolkien"},
        {"book_id": 37, "title": "Harry Potter and the Sorcerer's Stone", "number_of_pages": 320, "author_id": 17,
         "first_name": "J.K.", "last_name": "Rowling"},
        {"book_id": 38, "title": "The Hobbit", "number_of_pages": 310, "author_id": 16, "first_name": "J.R.R.",
         "last_name": "Tolkien"},
        {"book_id": 39, "title": "Fahrenheit 451", "number_of_pages": 158, "author_id": 18, "first_name": "Ray",
         "last_name": "Bradbury"},
        {"book_id": 40, "title": "Crime and Punishment", "number_of_pages": 671, "author_id": 19,
         "first_name": "Fyodor", "last_name": "Dostoevsky"},
        {"book_id": 41, "title": "Moby-Dick", "number_of_pages": 635, "author_id": 20, "first_name": "Herman",
         "last_name": "Melville"},
        {"book_id": 42, "title": "Jane Eyre", "number_of_pages": 507, "author_id": 14, "first_name": "Charlotte",
         "last_name": "Brontë"},
        {"book_id": 43, "title": "The Odyssey", "number_of_pages": 475, "author_id": 21, "first_name": "Homer",
         "last_name": "Homer"},
        {"book_id": 44, "title": "The Chronicles of Narnia", "number_of_pages": 767, "author_id": 22,
         "first_name": "C.S.", "last_name": "Lewis"},
        {"book_id": 45, "title": "Brave New World", "number_of_pages": 288, "author_id": 23, "first_name": "Aldous",
         "last_name": "Huxley"},
        {"book_id": 46, "title": "The Shining", "number_of_pages": 447, "author_id": 24, "first_name": "Stephen",
         "last_name": "King"},
        {"book_id": 47, "title": "The Hitchhiker's Guide to the Galaxy", "number_of_pages": 193, "author_id": 25,
         "first_name": "Douglas", "last_name": "Adams"},
        {"book_id": 48, "title": "The Kite Runner", "number_of_pages": 371, "author_id": 26, "first_name": "Khaled",
         "last_name": "Hosseini"},
        {"book_id": 49, "title": "Anna Karenina", "number_of_pages": 864, "author_id": 27, "first_name": "Leo",
         "last_name": "Tolstoy"},
        {"book_id": 50, "title": "The Road", "number_of_pages": 241, "author_id": 28, "first_name": "Cormac",
         "last_name": "McCarthy"},
        {"book_id": 51, "title": "Wuthering Heights", "number_of_pages": 416, "author_id": 24, "first_name": "Emily",
         "last_name": "Brontë"},
        {"book_id": 52, "title": "Slaughterhouse-Five", "number_of_pages": 215, "author_id": 29, "first_name": "Kurt",
         "last_name": "Vonnegut"},
        {"book_id": 53, "title": "The Picture of Dorian Gray", "number_of_pages": 254, "author_id": 30,
         "first_name": "Oscar", "last_name": "Wilde"},
        {"book_id": 54, "title": "The Count of Monte Cristo", "number_of_pages": 1276, "author_id": 31,
         "first_name": "Alexandre", "last_name": "Dumas"},
        {"book_id": 55, "title": "Lord of the Flies", "number_of_pages": 224, "author_id": 32, "first_name": "William",
         "last_name": "Golding"}]

random.shuffle(data)  # Used just to obtain/test different datasets case scenarios

# print(len(books_insert_stmt))
# print(len(authors_insert_stmt))

with Session(engine) as session:
    for entry in data:
        with session.begin():
            if not session.query(Books).filter(
                    (Books.title == entry['title']) & (Books.number_of_pages == entry['number_of_pages'])).first():
                try:
                    session.add(
                        Books(id=entry['book_id'], title=entry['title'], number_of_pages=entry['number_of_pages']))

                    if not session.query(Authors).filter(
                            (Authors.first_name == entry['first_name']) & (
                                    Authors.last_name == entry['last_name'])).first() and not session.query(
                        Authors).filter(
                        (Authors.id == entry['author_id'])).first():
                        session.add(Authors(id=entry['author_id'], first_name=entry['first_name'],
                                            last_name=entry['last_name']))
                except IntegrityError as e:
                    print(e)
                    session.rollback()
                else:
                    session.commit()

    for entry in data:
        author_idd = select(Authors.id).where(
            (Authors.first_name == entry['first_name']) & (Authors.last_name == entry['last_name'])).scalar_subquery()
        book_idd = select(Books.id).where(
            (Books.title == entry['title']) & (Books.number_of_pages == entry['number_of_pages'])).scalar_subquery()
        author_idd_null_check = session.query(Authors.id).where(
            (Authors.first_name == entry['first_name']) & (Authors.last_name == entry['last_name'])).first()
        author_book_validity_check = session.query(Author_books).filter(
            (Author_books.author_id == author_idd) & (Author_books.book_id == book_idd)).first()
        book_validity_check = session.query(Author_books).filter((Author_books.book_id == book_idd)).first()
        # print(author_book_validity_check, '\n')
        if not book_validity_check and not author_book_validity_check and author_idd_null_check:
            # print(author_idd)
            # print(type(author_idd))
            try:
                session.add(Author_books(author_id=author_idd, book_id=book_idd))
            except IntegrityError as e:
                print(e)
                session.rollback()
            else:
                session.commit()

    # Query the database and select all records in the Authors table, then order by ID.
    authors_data = session.query(Authors).order_by('id').all()
    print([tuple(getattr(row, item) for item in ('id', 'first_name', 'last_name')) for row in authors_data], '\n')

    # Query the database and select all records in the Books table, then order by ID.
    books_data = session.query(Books).order_by('id').all()
    print([tuple(getattr(row, item) for item in ('id', 'title', 'number_of_pages')) for row in books_data], '\n')

    # Query the database and select all records in the Author_books table.
    author_books_data = session.query(Author_books).all()
    print([tuple(getattr(row, item) for item in ('id', 'author_id', 'book_id')) for row in author_books_data])
