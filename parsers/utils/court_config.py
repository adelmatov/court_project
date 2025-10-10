URL = 'https://office.sud.kz/new/scheduleOfCases/index.xhtml'


headers = {
    'base': {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-AU,en;q=0.9",
        "connection": "keep-alive",
        "host": "office.sud.kz",
        "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Microsoft Edge\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-webapp": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
    },
    'post': {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-AU,en;q=0.9",
        "connection": "keep-alive",
        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        "faces-request": "partial/ajax",
        "host": "office.sud.kz",
        "origin": "https://office.sud.kz",
        "referer": "https://office.sud.kz/new/scheduleOfCases/index.xhtml",
        "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Microsoft Edge\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-webapp": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    }
}

payload = {
        'region': {
            "j_idt34:schedule-of-case-filter-form": "j_idt34:schedule-of-case-filter-form",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field": "2",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field": "",
            "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field": "",
            "javax.faces.ViewState": "",
            "javax.faces.source": "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field",
            "javax.faces.partial.event": "change",
            "javax.faces.partial.execute": "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field @component",
            "javax.faces.partial.render": "@component",
            "javax.faces.behavior.event": "change",
            "org.richfaces.ajax.component": "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1",
            "javax.faces.partial.ajax": "true"
        },
        'court': {
            "j_idt34:schedule-of-case-filter-form": "j_idt34:schedule-of-case-filter-form",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field": "2",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field": "413",
            "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field": "",
            "javax.faces.ViewState": "",
            "javax.faces.source": "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field",
            "javax.faces.partial.event": "change",
            "javax.faces.partial.execute": "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field @component",
            "javax.faces.partial.render": "@component",
            "javax.faces.behavior.event": "change",
            "org.richfaces.ajax.component": "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1",
            "javax.faces.partial.ajax": "true"
        },
        'case': {
            "j_idt34:schedule-of-case-filter-form": "j_idt34:schedule-of-case-filter-form",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field": "2",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field": "413",
            "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field": "APPK",
            "javax.faces.ViewState": "",
            "javax.faces.source": "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field",
            "javax.faces.partial.event": "change",
            "javax.faces.partial.execute": "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field @component",
            "javax.faces.partial.render": "@component",
            "javax.faces.behavior.event": "change",
            "org.richfaces.ajax.component": "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1",
            "javax.faces.partial.ajax": "true"
        },
        'date': {
            "j_idt34:schedule-of-case-filter-form": "j_idt34:schedule-of-case-filter-form",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field": "2",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field": "413",
            "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field": "APPK",
            "j_idt34:schedule-of-case-filter-form:j_idt53:date-field": "",
            "javax.faces.ViewState": "",
            "javax.faces.source": "j_idt34:schedule-of-case-filter-form:j_idt53:date-field",
            "javax.faces.partial.event": "change",
            "javax.faces.partial.execute": "j_idt34:schedule-of-case-filter-form:j_idt53:date-field @component",
            "javax.faces.partial.render": "@component",
            "javax.faces.behavior.event": "change",
            "org.richfaces.ajax.component": "j_idt34:schedule-of-case-filter-form:j_idt53:date-field",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1",
            "javax.faces.partial.ajax": "true"
        },
        'search': {
            "j_idt34:schedule-of-case-filter-form": "j_idt34:schedule-of-case-filter-form",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field": "2",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field": "413",
            "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field": "APPK",
            "j_idt34:schedule-of-case-filter-form:j_idt53:date-field": "",
            "javax.faces.ViewState": "",
            "javax.faces.source": "j_idt34:schedule-of-case-filter-form:j_idt56:search",
            "javax.faces.partial.event": "click",
            "javax.faces.partial.execute": "j_idt34:schedule-of-case-filter-form:j_idt56:search @component",
            "javax.faces.partial.render": "@component",
            "org.richfaces.ajax.component": "j_idt34:schedule-of-case-filter-form:j_idt56:search",
            "j_idt34:schedule-of-case-filter-form:j_idt56:search": "j_idt34:schedule-of-case-filter-form:j_idt56:search",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1",
            "javax.faces.partial.ajax": "true"
        },
        'final': {
            "j_idt34:schedule-of-case-filter-form": "j_idt34:schedule-of-case-filter-form",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt37:district-field": "2",
            "j_idt34:schedule-of-case-filter-form:j_idt36:j_idt41:court-field": "413",
            "j_idt34:schedule-of-case-filter-form:j_idt45:caseType-field": "APPK",
            "j_idt34:schedule-of-case-filter-form:j_idt53:date-field": "",
            "javax.faces.ViewState": "",
            "javax.faces.source": "j_idt34:schedule-of-case-filter-form:j_idt56:j_idt57",
            "javax.faces.partial.execute": "j_idt34:schedule-of-case-filter-form:j_idt56:j_idt57 @component",
            "javax.faces.partial.render": "@component",
            "org.richfaces.ajax.component": "j_idt34:schedule-of-case-filter-form:j_idt56:j_idt57",
            "j_idt34:schedule-of-case-filter-form:j_idt56:j_idt57": "j_idt34:schedule-of-case-filter-form:j_idt56:j_idt57",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1",
            "javax.faces.partial.ajax": "true"
        },
        'lang': {
            "f_l_temp": "",
            "javax.faces.ViewState": "",
            "javax.faces.source": "f_l_temp:js_temp_1",
            "javax.faces.partial.execute": "f_l_temp:js_temp_1 @component",
            "javax.faces.partial.render": "@component",
            "param1": "https://office.sud.kz/new/scheduleOfCases/index.xhtml",
            "org.richfaces.ajax.component": "f_l_temp:js_temp_1",
            "f_l_temp:js_temp_1": "",
            "rfExt": "null",
            "AJAX:EVENTS_COUNT": "1"
        }
}


