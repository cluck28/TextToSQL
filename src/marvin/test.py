from marvin import ai_model
from pydantic import BaseModel, Field


@ai_model
class Location(BaseModel):
    """A representation of a US city and state"""

    city: str = Field(description="The city's proper name")
    state: str = Field(description="The state's two-letter abbreviation (e.g. NY)")

# We can now put pass unstructured context to this model.
if __name__ == '__main__':
    test = Location("The Big Apple")
    print(test)
