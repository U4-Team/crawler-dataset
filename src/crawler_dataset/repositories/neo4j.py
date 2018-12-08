import pandas

from core.repositories import BaseNeo4jRepository


class Neo4jRepository(BaseNeo4jRepository):
    def store_dataset_row(self, dataset_row: pandas.Series):
        # self.logger.info(dataset_row.to_dict())
        dataset_dict = dataset_row.to_dict()
        company_properties = {
            k: v 
            for k, v in dataset_dict.items() 
            if k not in ['CEO', 'LEGAL_ADDRESS', 'SITE', 'EMAIL', 'PHONES']
        }
        self.merge_node('SPARK_ID', ['Company'], company_properties)

        if dataset_dict['CEO']:
            ceo_properties = {
                'FULL_NAME': dataset_dict['CEO']
            }
            self.merge_node('FULL_NAME', ['Person'], ceo_properties)
            self.merge_directed_edge(
                left_node_by_key='SPARK_ID',
                left_node_by=company_properties,
                edge_label='CEO_OF',
                right_node_by_key='FULL_NAME',
                right_node_by=ceo_properties
            )

        if dataset_dict['LEGAL_ADDRESS']:
            legal_address_properties = {
                'LEGAL_ADDRESS': dataset_dict['LEGAL_ADDRESS']
            }
            self.merge_node('LEGAL_ADDRESS', ['Address'], legal_address_properties)
            self.merge_directed_edge(
                left_node_by_key='SPARK_ID',
                left_node_by=company_properties,
                edge_label='LEGAL_ADDRESS_OF',
                right_node_by_key='LEGAL_ADDRESS',
                right_node_by=legal_address_properties
            )

        if dataset_dict['SITE']:
            site_properties = {
                'SITE': dataset_dict['SITE']
            }
            self.merge_node('SITE', ['Url'], site_properties)
            self.merge_directed_edge(
                left_node_by_key='SPARK_ID',
                left_node_by=company_properties,
                edge_label='SITE_OF',
                right_node_by_key='SITE',
                right_node_by=site_properties
            )

        if dataset_dict['EMAIL']:
            email_properties = {
                'EMAIL': dataset_dict['EMAIL']
            }
            self.merge_node('EMAIL', ['Email'], email_properties)
            self.merge_directed_edge(
                left_node_by_key='SPARK_ID',
                left_node_by=company_properties,
                edge_label='EMAIL_OF',
                right_node_by_key='EMAIL',
                right_node_by=email_properties
            )

        if dataset_dict['PHONES']:
            for phone in dataset_dict['PHONES']:
                phone_property = {
                    'PHONE': phone
                }
                self.merge_node('PHONE', ['Phone'], phone_property)
                self.merge_directed_edge(
                    left_node_by_key='SPARK_ID',
                    left_node_by=company_properties,
                    edge_label='PHONE_OF',
                    right_node_by_key='PHONE',
                    right_node_by=phone_property
                )
        