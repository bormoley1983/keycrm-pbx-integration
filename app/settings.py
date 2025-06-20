from pydantic_settings import BaseSettings
from pydantic import AnyHttpUrl, Field


class Settings(BaseSettings):
    # --- Asterisk / AMI ----------------------------------------------------
    asterisk_host: str = "127.0.0.1"
    asterisk_port: int = 5038
    ami_user: str
    ami_secret: str

    # --- KeyCRM ------------------------------------------------------------
#    keycrm_webhook_url: AnyHttpUrl = Field(
#        "https://example.keycrm.app/asterisk/webhook"
#    )

    keycrm_webhook_url: str | None = Field(
        default=None,
        description="Full https://… URL provided by KeyCRM › Integrations › Asterisk",
    )
    # --- Misc --------------------------------------------------------------
    log_level: str = "INFO"

    class Config:
        env_file = ".env"           # read from project root
        case_sensitive = False


settings = Settings()               # import this everywhere
