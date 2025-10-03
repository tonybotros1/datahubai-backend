from pydantic import BaseModel, EmailStr


class CompanyCreate(BaseModel):
    name: str
    owner_email: EmailStr
    owner_password: str


class InviteCreate(BaseModel):
    email: EmailStr
    role: str  # validate in real code
