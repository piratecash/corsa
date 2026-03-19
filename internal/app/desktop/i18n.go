package desktop

import (
	"fmt"
	"strings"
)

type languageOption struct {
	Code  string
	Label string
}

var supportedLanguages = []languageOption{
	{Code: "en", Label: "EN"},
	{Code: "ru", Label: "RU"},
	{Code: "es", Label: "ES"},
	{Code: "fr", Label: "FR"},
	{Code: "ar", Label: "AR"},
	{Code: "zh", Label: "中文"},
}

var messages = map[string]map[string]string{
	"en": {
		"app.title":                "Corsa Desktop",
		"app.subtitle":             "DEX Messenger",
		"header.language":          "Language",
		"status.compose_default":   "Compose a direct message to a known fingerprint.",
		"status.sending":           "Sending...",
		"status.send_failed":       "Send failed: %s",
		"status.message_sent":      "Message sent.",
		"status.syncing":           "Syncing peer messages...",
		"status.sync_done":         "Peer sync complete. Imported: %d",
		"status.sync_failed":       "Peer sync failed: %s",
		"status.chat_selected":     "Chat selected.",
		"clients.title":            "Clients",
		"clients.you":              "You: %s",
		"clients.known":            "Known identities: %d",
		"clients.empty":            "No remote identities discovered yet.",
		"node.title":               "Node",
		"node.client_version":      "Version: %s",
		"node.peer_version":        "Peer version: %s",
		"node.listen":              "Listen: %s",
		"node.type":                "Node type: %s",
		"node.services":            "Services: %s",
		"node.connected":           "Connected: %t",
		"node.peers":               "Peers: %d",
		"node.checked":             "Checked: %s",
		"node.error":               "Local node error: %s",
		"node.error.none":          "none",
		"chat.title":               "Chat",
		"chat.with":                "Chat with %s",
		"chat.choose":              "Choose a client on the left to open a direct chat.",
		"chat.fingerprint":         "Fingerprint: %s",
		"chat.peers":               "Known peers: %d",
		"chat.empty":               "No direct messages with this identity yet.",
		"chat.you":                 "You: %s",
		"chat.sync":                "Sync Peer Messages",
		"chat.sync_disabled":       "No peers to sync",
		"compose.title":            "Message",
		"compose.recipient.select": "Recipient: select a client from the left list.",
		"compose.recipient.value":  "Recipient: %s",
		"compose.send":             "Send Message",
		"compose.select_first":     "Select Client First",
		"compose.body":             "Message body",
		"compose.placeholder":      "Write an encrypted message",
	},
	"ru": {
		"app.title":                "Corsa Desktop",
		"app.subtitle":             "DEX Мессенджер",
		"header.language":          "Язык",
		"status.compose_default":   "Напишите direct-сообщение известному fingerprint.",
		"status.sending":           "Отправка...",
		"status.send_failed":       "Ошибка отправки: %s",
		"status.message_sent":      "Сообщение отправлено.",
		"status.syncing":           "Синхронизация сообщений у пира...",
		"status.sync_done":         "Синхронизация завершена. Импортировано: %d",
		"status.sync_failed":       "Ошибка синхронизации: %s",
		"status.chat_selected":     "Чат выбран.",
		"clients.title":            "Клиенты",
		"clients.you":              "Вы: %s",
		"clients.known":            "Известных identity: %d",
		"clients.empty":            "Удаленные identity пока не обнаружены.",
		"node.title":               "Узел",
		"node.client_version":      "Версия: %s",
		"node.peer_version":        "Версия пира: %s",
		"node.listen":              "Прослушивание: %s",
		"node.type":                "Тип узла: %s",
		"node.services":            "Сервисы: %s",
		"node.connected":           "Подключен: %t",
		"node.peers":               "Пиры: %d",
		"node.checked":             "Проверено: %s",
		"node.error":               "Ошибка локальной ноды: %s",
		"node.error.none":          "нет",
		"chat.title":               "Чат",
		"chat.with":                "Чат с %s",
		"chat.choose":              "Выберите клиента слева, чтобы открыть direct-чат.",
		"chat.fingerprint":         "Fingerprint: %s",
		"chat.peers":               "Известных пиров: %d",
		"chat.empty":               "Direct-сообщений с этим identity пока нет.",
		"chat.you":                 "Вы: %s",
		"chat.sync":                "Синхронизировать сообщения",
		"chat.sync_disabled":       "Нет пиров для синка",
		"compose.title":            "Сообщение",
		"compose.recipient.select": "Получатель: выберите клиента в списке слева.",
		"compose.recipient.value":  "Получатель: %s",
		"compose.send":             "Отправить сообщение",
		"compose.select_first":     "Сначала выберите клиента",
		"compose.body":             "Текст сообщения",
		"compose.placeholder":      "Напишите зашифрованное сообщение",
	},
	"es": {
		"app.title":                "Corsa Desktop",
		"app.subtitle":             "Mensajero DEX",
		"header.language":          "Idioma",
		"status.compose_default":   "Escribe un mensaje directo para una huella conocida.",
		"status.sending":           "Enviando...",
		"status.send_failed":       "Error al enviar: %s",
		"status.message_sent":      "Mensaje enviado.",
		"status.syncing":           "Sincronizando mensajes del peer...",
		"status.sync_done":         "Sincronización completada. Importados: %d",
		"status.sync_failed":       "Error de sincronización: %s",
		"status.chat_selected":     "Chat seleccionado.",
		"clients.title":            "Clientes",
		"clients.you":              "Tú: %s",
		"clients.known":            "Identidades conocidas: %d",
		"clients.empty":            "Aún no se descubrieron identidades remotas.",
		"node.title":               "Nodo",
		"node.client_version":      "Versión: %s",
		"node.peer_version":        "Versión del peer: %s",
		"node.listen":              "Escucha: %s",
		"node.type":                "Tipo de nodo: %s",
		"node.services":            "Servicios: %s",
		"node.connected":           "Conectado: %t",
		"node.peers":               "Peers: %d",
		"node.checked":             "Comprobado: %s",
		"node.error":               "Error del nodo local: %s",
		"node.error.none":          "ninguno",
		"chat.title":               "Chat",
		"chat.with":                "Chat con %s",
		"chat.choose":              "Elige un cliente a la izquierda para abrir un chat directo.",
		"chat.fingerprint":         "Huella: %s",
		"chat.peers":               "Peers conocidos: %d",
		"chat.empty":               "Todavía no hay mensajes directos con esta identidad.",
		"chat.you":                 "Tú: %s",
		"chat.sync":                "Sincronizar mensajes",
		"chat.sync_disabled":       "No hay peers para sincronizar",
		"compose.title":            "Mensaje",
		"compose.recipient.select": "Destinatario: elige un cliente de la lista izquierda.",
		"compose.recipient.value":  "Destinatario: %s",
		"compose.send":             "Enviar mensaje",
		"compose.select_first":     "Primero elige un cliente",
		"compose.body":             "Texto del mensaje",
		"compose.placeholder":      "Escribe un mensaje cifrado",
	},
	"fr": {
		"app.title":                "Corsa Desktop",
		"app.subtitle":             "Messagerie DEX",
		"header.language":          "Langue",
		"status.compose_default":   "Rédigez un message direct pour une empreinte connue.",
		"status.sending":           "Envoi...",
		"status.send_failed":       "Échec de l'envoi : %s",
		"status.message_sent":      "Message envoyé.",
		"status.syncing":           "Synchronisation des messages du pair...",
		"status.sync_done":         "Synchronisation terminée. Importés : %d",
		"status.sync_failed":       "Échec de la synchronisation : %s",
		"status.chat_selected":     "Chat sélectionné.",
		"clients.title":            "Clients",
		"clients.you":              "Vous : %s",
		"clients.known":            "Identités connues : %d",
		"clients.empty":            "Aucune identité distante détectée pour l'instant.",
		"node.title":               "Nœud",
		"node.client_version":      "Version : %s",
		"node.peer_version":        "Version du pair : %s",
		"node.listen":              "Écoute : %s",
		"node.type":                "Type de nœud : %s",
		"node.services":            "Services : %s",
		"node.connected":           "Connecté : %t",
		"node.peers":               "Pairs : %d",
		"node.checked":             "Vérifié : %s",
		"node.error":               "Erreur du nœud local : %s",
		"node.error.none":          "aucune",
		"chat.title":               "Chat",
		"chat.with":                "Chat avec %s",
		"chat.choose":              "Choisissez un client à gauche pour ouvrir un chat direct.",
		"chat.fingerprint":         "Empreinte : %s",
		"chat.peers":               "Pairs connus : %d",
		"chat.empty":               "Aucun message direct avec cette identité pour l'instant.",
		"chat.you":                 "Vous : %s",
		"chat.sync":                "Synchroniser les messages",
		"chat.sync_disabled":       "Aucun pair à synchroniser",
		"compose.title":            "Message",
		"compose.recipient.select": "Destinataire : choisissez un client dans la liste de gauche.",
		"compose.recipient.value":  "Destinataire : %s",
		"compose.send":             "Envoyer le message",
		"compose.select_first":     "Choisissez d'abord un client",
		"compose.body":             "Corps du message",
		"compose.placeholder":      "Écrivez un message chiffré",
	},
	"ar": {
		"app.title":                "Corsa Desktop",
		"app.subtitle":             "رسائل DEX",
		"header.language":          "اللغة",
		"status.compose_default":   "اكتب رسالة مباشرة إلى بصمة معروفة.",
		"status.sending":           "جارٍ الإرسال...",
		"status.send_failed":       "فشل الإرسال: %s",
		"status.message_sent":      "تم إرسال الرسالة.",
		"status.syncing":           "جارٍ مزامنة رسائل النظير...",
		"status.sync_done":         "اكتملت المزامنة. تم استيراد: %d",
		"status.sync_failed":       "فشلت المزامنة: %s",
		"status.chat_selected":     "تم اختيار المحادثة.",
		"clients.title":            "العملاء",
		"clients.you":              "أنت: %s",
		"clients.known":            "الهويات المعروفة: %d",
		"clients.empty":            "لم يتم اكتشاف هويات بعيدة بعد.",
		"node.title":               "العقدة",
		"node.client_version":      "الإصدار: %s",
		"node.peer_version":        "إصدار النظير: %s",
		"node.listen":              "الاستماع: %s",
		"node.type":                "نوع العقدة: %s",
		"node.services":            "الخدمات: %s",
		"node.connected":           "متصل: %t",
		"node.peers":               "الأقران: %d",
		"node.checked":             "آخر فحص: %s",
		"node.error":               "خطأ العقدة المحلية: %s",
		"node.error.none":          "لا يوجد",
		"chat.title":               "الدردشة",
		"chat.with":                "الدردشة مع %s",
		"chat.choose":              "اختر عميلاً من اليسار لفتح دردشة مباشرة.",
		"chat.fingerprint":         "البصمة: %s",
		"chat.peers":               "الأقران المعروفون: %d",
		"chat.empty":               "لا توجد رسائل مباشرة مع هذه الهوية بعد.",
		"chat.you":                 "أنت: %s",
		"chat.sync":                "مزامنة الرسائل",
		"chat.sync_disabled":       "لا يوجد أقران للمزامنة",
		"compose.title":            "الرسالة",
		"compose.recipient.select": "المستلم: اختر عميلاً من القائمة اليسرى.",
		"compose.recipient.value":  "المستلم: %s",
		"compose.send":             "إرسال الرسالة",
		"compose.select_first":     "اختر عميلاً أولاً",
		"compose.body":             "نص الرسالة",
		"compose.placeholder":      "اكتب رسالة مشفرة",
	},
	"zh": {
		"app.title":                "Corsa Desktop",
		"app.subtitle":             "DEX 消息客户端",
		"header.language":          "语言",
		"status.compose_default":   "向已知指纹编写一条直连消息。",
		"status.sending":           "发送中...",
		"status.send_failed":       "发送失败：%s",
		"status.message_sent":      "消息已发送。",
		"status.syncing":           "正在同步对端消息...",
		"status.sync_done":         "同步完成。已导入：%d",
		"status.sync_failed":       "同步失败：%s",
		"status.chat_selected":     "已选择聊天。",
		"clients.title":            "客户端",
		"clients.you":              "你：%s",
		"clients.known":            "已知身份：%d",
		"clients.empty":            "尚未发现远程身份。",
		"node.title":               "节点",
		"node.client_version":      "版本：%s",
		"node.peer_version":        "对端版本：%s",
		"node.listen":              "监听：%s",
		"node.type":                "节点类型：%s",
		"node.services":            "服务：%s",
		"node.connected":           "已连接：%t",
		"node.peers":               "对等节点：%d",
		"node.checked":             "检查时间：%s",
		"node.error":               "本地节点错误：%s",
		"node.error.none":          "无",
		"chat.title":               "聊天",
		"chat.with":                "与 %s 聊天",
		"chat.choose":              "请先在左侧选择一个客户端以打开直连聊天。",
		"chat.fingerprint":         "指纹：%s",
		"chat.peers":               "已知对等节点：%d",
		"chat.empty":               "与该身份之间还没有直连消息。",
		"chat.you":                 "你：%s",
		"chat.sync":                "同步消息",
		"chat.sync_disabled":       "没有可同步的对等节点",
		"compose.title":            "消息",
		"compose.recipient.select": "接收方：请先从左侧列表选择客户端。",
		"compose.recipient.value":  "接收方：%s",
		"compose.send":             "发送消息",
		"compose.select_first":     "请先选择客户端",
		"compose.body":             "消息内容",
		"compose.placeholder":      "输入加密消息",
	},
}

func normalizeLanguage(code string) string {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "ru", "es", "fr", "ar", "zh":
		return strings.ToLower(strings.TrimSpace(code))
	default:
		return "en"
	}
}

func translate(lang, key string, args ...any) string {
	lang = normalizeLanguage(lang)
	if value, ok := messages[lang][key]; ok {
		if len(args) > 0 {
			return fmt.Sprintf(value, args...)
		}
		return value
	}
	if value, ok := messages["en"][key]; ok {
		if len(args) > 0 {
			return fmt.Sprintf(value, args...)
		}
		return value
	}
	return key
}

func currentLanguageLabel(code string) string {
	code = normalizeLanguage(code)
	for _, option := range supportedLanguages {
		if option.Code == code {
			return option.Label
		}
	}
	return "EN"
}

func localizedLanguageName(code string) string {
	switch normalizeLanguage(code) {
	case "ru":
		return "Русский"
	case "es":
		return "Español"
	case "fr":
		return "Français"
	case "ar":
		return "العربية"
	case "zh":
		return "中文"
	default:
		return "English"
	}
}
