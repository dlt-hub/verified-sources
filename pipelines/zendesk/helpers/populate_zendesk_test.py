from zenpy import Zenpy
from zenpy.lib.api_objects import Ticket, User

current_increment = 997
def add_users_and_tickets(zen_client: Zenpy):
    main_requester_id = zen_client.users.me().id

    for i in range(10):
        zen_client.users.create(
            User(name=f"user{i+current_increment}", email=f"user{i+current_increment}@example.com")
        )

        zen_client.tickets.create(
            Ticket(description=f"test{i+current_increment}", requester_id=main_requester_id)
        )
