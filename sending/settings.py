from pydantic import BaseSettings


class Settings(BaseSettings):
    enable_logging: bool = False

    class Config:
        env_prefix = "sending__"


settings = Settings()
