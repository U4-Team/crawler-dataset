from typing import List,Dict
import pandas

from core.repositories import BaseMongoRepository


class MongoRepository(BaseMongoRepository):
    def store_companies(self, companies: List[Dict]):
        self.db.companies.insert_many(companies)