mock_companies_data = {'results': [{'id': '7086461639',
                                    'properties': {'createdate': '2023-02-24T03:00:14.155Z', 'domain': 'hubspot.com', 'hs_lastmodifieddate': '2023-02-24T03:00:26.696Z', 'hs_object_id': '7086461639',
                                                   'name': 'Hubspot, Inc.'}, 'createdAt': '2023-02-24T03:00:14.155Z', 'updatedAt': '2023-02-24T03:00:26.696Z', 'archived': False},
                                   {'id': '7086464459',
                                    'properties': {'createdate': '2023-02-24T03:25:40.027Z', 'domain': 'fda.gov', 'hs_lastmodifieddate': '2023-02-24T03:25:53.252Z', 'hs_object_id': '7086464459',
                                                   'name': 'U.S. Food and Drug Administration'}, 'createdAt': '2023-02-24T03:25:40.027Z', 'updatedAt': '2023-02-24T03:25:53.252Z', 'archived': False}]
                       }

mock_contacts_data = {'results': [{'id': '1',
                                   'properties': {'createdate': '2023-02-24T03:00:13.460Z', 'email': 'emailmaria@hubspot.com', 'firstname': 'Maria', 'hs_object_id': '1',
                                                  'lastmodifieddate': '2023-02-24T14:07:59.929Z', 'lastname': 'Johnson (Sample Contact)'}, 'createdAt': '2023-02-24T03:00:13.460Z',
                                   'updatedAt': '2023-02-24T14:07:59.929Z', 'archived': False},
                                  {'id': '51',
                                   'properties': {'createdate': '2023-02-24T03:00:13.789Z', 'email': 'bh@hubspot.com',
                                                  'firstname': 'Brian', 'hs_object_id': '51',
                                                  'lastmodifieddate': '2023-02-24T03:00:25.112Z',
                                                  'lastname': 'Halligan (Sample Contact)'},
                                   'createdAt': '2023-02-24T03:00:13.789Z', 'updatedAt': '2023-02-24T03:00:25.112Z', 'archived': False}],
                      }

mock_deals_data = {'results': [{'id': '6744192456', 'properties': {'amount': '500', 'closedate': None, 'createdate': '2023-02-24T03:01:01.147Z', 'dealname': 'With Atul', 'dealstage': 'qualifiedtobuy',
                                                                   'hs_lastmodifieddate': '2023-02-24T03:01:01.711Z', 'hs_object_id': '6744192456', 'pipeline': 'default'},
                                'createdAt': '2023-02-24T03:01:01.147Z', 'updatedAt': '2023-02-24T03:01:01.711Z', 'archived': False}, {'id': '6744457152',
                                                                                                                                       'properties': {'amount': '3233', 'closedate': None,
                                                                                                                                                      'createdate': '2023-02-24T03:53:39.801Z',
                                                                                                                                                      'dealname': 'Yodoo Deal (2021-11-25)',
                                                                                                                                                      'dealstage': 'qualifiedtobuy',
                                                                                                                                                      'hs_lastmodifieddate': '2023-02-24T03:53:43.118Z',
                                                                                                                                                      'hs_object_id': '6744457152',
                                                                                                                                                      'pipeline': 'default'},
                                                                                                                                       'createdAt': '2023-02-24T03:53:39.801Z',
                                                                                                                                       'updatedAt': '2023-02-24T03:53:43.118Z', 'archived': False}]}

mock_products_data = {'results': [{'id': '1155995587', 'properties': {'createdate': '2023-02-24T04:00:20.898Z', 'description': 'Inner Garment', 'hs_lastmodifieddate': '2023-02-24T04:00:20.898Z',
                                                                      'hs_object_id': '1155995587', 'name': 'Jockey ineer', 'price': '23'}, 'createdAt': '2023-02-24T04:00:20.898Z',
                                   'updatedAt': '2023-02-24T04:00:20.898Z', 'archived': False}, {'id': '1156116944', 'properties': {'createdate': '2023-02-24T04:04:14.269Z', 'description': None,
                                                                                                                                    'hs_lastmodifieddate': '2023-02-24T04:04:14.269Z',
                                                                                                                                    'hs_object_id': '1156116944', 'name': 'Mayonnaise - Individual Pkg',
                                                                                                                                    'price': '60'}, 'createdAt': '2023-02-24T04:04:14.269Z',
                                                                                                 'updatedAt': '2023-02-24T04:04:14.269Z', 'archived': False}]}

mock_tickets_data = {'results': [{'id': '1075966440',
                                  'properties': {'content': 'New Ticket', 'createdate': '2023-01-15T05:00:00Z', 'hs_lastmodifieddate': '2023-02-24T05:01:27.700Z', 'hs_object_id': '1075966440',
                                                 'hs_pipeline': '0', 'hs_pipeline_stage': '1', 'hs_ticket_category': None, 'hs_ticket_priority': 'HIGH', 'subject': 'Barry Smallridge'},
                                  'createdAt': '2023-01-15T05:00:00Z', 'updatedAt': '2023-02-24T05:01:27.700Z', 'archived': False}, {'id': '1075966441', 'properties': {'content': 'Updated Ticket',
                                                                                                                                                                        'createdate': '2022-12-04T05:00:00Z',
                                                                                                                                                                        'hs_lastmodifieddate': '2023-02-24T05:01:28.161Z',
                                                                                                                                                                        'hs_object_id': '1075966441',
                                                                                                                                                                        'hs_pipeline': '0',
                                                                                                                                                                        'hs_pipeline_stage': '4',
                                                                                                                                                                        'hs_ticket_category': None,
                                                                                                                                                                        'hs_ticket_priority': 'HIGH',
                                                                                                                                                                        'subject': 'Griffith Ticehurst'},
                                                                                                                                     'createdAt': '2022-12-04T05:00:00Z',
                                                                                                                                     'updatedAt': '2023-02-24T05:01:28.161Z', 'archived': False}]}

mock_quotes_data = {'results': [{'id': '70524908',
                                 'properties': {'hs_createdate': '2023-02-24T05:03:50.166Z', 'hs_expiration_date': '2023-05-26T03:59:59.999Z', 'hs_lastmodifieddate': '2023-02-24T05:03:50.923Z',
                                                'hs_object_id': '70524908', 'hs_public_url_key': 'dee8e054f6fd4f55aead6bd0811af068', 'hs_status': 'DRAFT', 'hs_title': 'New Quote'},
                                 'createdAt': '2023-02-24T05:03:50.166Z', 'updatedAt': '2023-02-24T05:03:50.923Z', 'archived': False}]}
