
START_URL = 'https://sso.stat.gov.kz/kclssofront/?client-id=3&backurl=/ru/cabinet/juridical/by/bin/&lang=ru'

RETRIES = 3

PLAYWRIGHT_LAUNCH_CONFIG = {
    "headless": False,
    "args": [
        "--disable-dev-shm-usage",
        "--disable-extensions",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-infobars",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-sync",
        "--metrics-recording-only",
        "--mute-audio",
        "--no-first-run",
        "--disable-notifications",
        "--disable-plugins",
        "--disable-application-cache",
    ]
}

PLAYWRIGHT_CONTEXT_CONFIG = {
    "viewport": {"width": 1920, "height": 1080},
    "device_scale_factor": 1,
    "accept_downloads": False,
    "ignore_https_errors": True,
    "bypass_csp": True,
    "java_script_enabled": False,
    "has_touch": False,
    "is_mobile": False,
    "geolocation": None,
    "permissions": [],
    "locale": "ru-RU",
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
}

FIELD_MAP_BIN = {
    "БИН": "bin",
    "Наименование": "name",
    "Дата регистрации": "registration_date",
    "Основной код ОКЭД": "main_oked",
    "Наименование вида экономической деятельности": "main_activity",
    "Вторичный код ОКЭД": "secondary_oked",
    "Код КРП (с учетом филиалов)": "krp_code_branch",
    "Наименование КРП": "krp_name",
    "Код КРП (без учета филиалов)": "krp_code_no_branch",
    "КАТО": "kato",
    "Юридический адрес": "legal_address",
    "Фамилия, имя, отчество руководителя": "director_name",
    "Код КФС": "kfs_code",
    "Наименование КФС": "kfs_name",
    "Код сектора экономики": "economy_sector_code",
    "Наименование сектора экономики": "economy_sector_name"
}

FIELD_MAP_IIN = {
    "ИИН": "iin",
    "Фамилия, имя, отчество руководителя": "director_name",
    "Наименование": "name",
    "Основной код ОКЭД": "main_oked",
    "Наименование вида экономической деятельности": "main_activity",
    "Вторичный код ОКЭД": "secondary_oked",
    "Код КРП (с учетом филиалов)": "krp_code_branch",
    "Наименование КРП": "krp_name",
    "КАТО": "kato",
    "Местонахождение ИП": "legal_address",
    "Код сектора экономики": "economy_sector_code",
    "Наименование сектора экономики": "economy_sector_name"
}

validatecert = {
    "revocationCheck": ["OCSP"],
    "certs": [
        "MIIERjCCA66gAwIBAgIUHoc3zcRJ0DbTnmdTfdcHAASFrakwDgYKKoMOAwoBAQIDAgUAMFgxSTBHBgNVBAMMQNKw0JvQotCi0KvSmiDQmtCj05jQm9CQ0J3QlNCr0KDQo9Co0Ksg0J7QoNCi0JDQm9Cr0pogKEdPU1QpIDIwMjIxCzAJBgNVBAYTAktaMB4XDTI1MDEyODA4MzE0M1oXDTI2MDEyODA4MzE0M1owgY8xJjAkBgNVBAMMHdCU0JXQm9Cs0JzQkNCi0J7QkiDQkNCb0JzQkNCXMRswGQYDVQQEDBLQlNCV0JvQrNCc0JDQotCe0JIxGDAWBgNVBAUTD0lJTjk0MTIwODM1MTQ2ODELMAkGA1UEBhMCS1oxITAfBgNVBCoMGNCj0KDQkNCX0JPQkNCb0JjQldCS0JjQpzCBrDAjBgkqgw4DCgEBAgIwFgYKKoMOAwoBAQICAQYIKoMOAwoBAwMDgYQABIGAU1P6ZuizOeqdQ0XutoLMbmPRNeJdvM0Ys9gZFaq+HiYK+7XOcOXTycVc5Mi70dKvIA/mboM54x3ygp2nxOc2nwKzE2G8MsIgWqH0G+p7WUzzlEwgmoplKsjgMsJ4QVxkgrR67q7NcDigEqMsTxTiPQ9T7lXPIfFrTM6uDc2loRKjggHEMIIBwDAOBgNVHQ8BAf8EBAMCA8gwHQYDVR0lBBYwFAYIKwYBBQUHAwQGCCqDDgMDBAEBMDgGA1UdIAQxMC8wLQYGKoMOAwMCMCMwIQYIKwYBBQUHAgEWFWh0dHA6Ly9wa2kuZ292Lmt6L2NwczA4BgNVHR8EMTAvMC2gK6AphidodHRwOi8vY3JsLnBraS5nb3Yua3ovbmNhX2dvc3RfMjAyMi5jcmwwOgYDVR0uBDMwMTAvoC2gK4YpaHR0cDovL2NybC5wa2kuZ292Lmt6L25jYV9kX2dvc3RfMjAyMi5jcmwwaAYIKwYBBQUHAQEEXDBaMCIGCCsGAQUFBzABhhZodHRwOi8vb2NzcC5wa2kuZ292Lmt6MDQGCCsGAQUFBzAChihodHRwOi8vcGtpLmdvdi5rei9jZXJ0L25jYV9nb3N0XzIwMjIuY2VyMB0GA1UdEQQWMBSBEjFhZGFtYW50ZW1AbWFpbC5ydTAdBgNVHQ4EFgQUHoc3zcRJ0DbTnmdTfdcHAASFrakwHwYDVR0jBBgwFoAU/jC+n8iQYz8f/1o8DLDIX0xtFwgwFgYGKoMOAwMFBAwwCgYIKoMOAwMFAQEwDgYKKoMOAwoBAQIDAgUAA4GBAGRYWYxn+GIsF06TGyopSper0VfotJHeDUVSY6i6nNTirwSUiNS/IIo+GIAH1+ETA8Nc5Zls8CoS+PlSUccraPFKEbiLPCWdMDKZ0V2p2bUbgomw/ZnMpfdFikEfcN41Sq+19gkAGGi5prEj179vM/ZduY/BHuypBK7rKNG9ZlOR"
    ]
}


# Данные для логина
LOGIN_PAYLOAD = {
    "username": "eHSyNGGR51RLtgk5sXiJoQ==",
    "password": "ajFvuIMG3wAu3wurtzOs0A==",
    "clientId": "3"
}

# URL'ы для редиректа
FIRST_REDIRECT_URL = (
    "https://stat.gov.kz/oauth/login"
    "?client-id=3"
    "&secret-code=f45556f8-4509-4937-b135-dc2bff94b3e2"
    "&backurl=%2Fru%2Fcabinet%2Fjuridical%2Fby%2Fbin%2F"
)

SECOND_REDIRECT_URL = (
    "https://stat.gov.kz/oauth/login/"
    "?client-id=3"
    "&secret-code=f45556f8-4509-4937-b135-dc2bff94b3e2"
    "&backurl=%2Fru%2Fcabinet%2Fjuridical%2Fby%2Fbin%2F"
)