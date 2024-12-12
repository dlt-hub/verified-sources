from pydantic import BaseModel, model_serializer


class MyBaseModel(BaseModel):
    @model_serializer(mode="wrap")
    def ser_model(self, nxt):
        if self.__class__.__qualname__ == "Attendee":
            return {
                "emailAddress": self.emailAddress,
                "person_id": getattr(self.person, "id", None),
            }
        else:
            return nxt(self)
