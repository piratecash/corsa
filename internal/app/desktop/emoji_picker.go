package desktop

import (
	"strings"
	"time"
	"unicode"

	"github.com/rs/zerolog/log"

	"gioui.org/io/key"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/widget"

	"github.com/piratecash/corsa/internal/app/desktop/ui"
)

type emojiCategoryID string

const (
	emojiCategoryRecent     emojiCategoryID = "recent"
	emojiCategorySmileys    emojiCategoryID = "smileys"
	emojiCategoryGestures   emojiCategoryID = "gestures"
	emojiCategoryAnimals    emojiCategoryID = "animals"
	emojiCategoryFood       emojiCategoryID = "food"
	emojiCategoryTravel     emojiCategoryID = "travel"
	emojiCategoryActivities emojiCategoryID = "activities"
	emojiCategorySymbols    emojiCategoryID = "symbols"
	emojiCategoryFlags      emojiCategoryID = "flags"
)

const (
	maxRecentEmojis      = 12
	emojiRecentSaveDelay = 500 * time.Millisecond
)

// The panel itself — its geometry, its widgets and the way they are drawn —
// lives in internal/app/desktop/ui. What stays here is what that package must
// not know: the CATALOGUE (which emoji exist and what they are called in six
// languages), the per-user "recently used" list persisted in preferences, and
// the way an open picker negotiates room with the on-screen keyboard.

type emojiEntry struct {
	value        string
	searchTokens []string
}

type emojiCategory struct {
	id      emojiCategoryID
	nameKey string
	entries []emojiEntry
}

var emojiCategories = []emojiCategory{
	{id: emojiCategorySmileys, nameKey: "emoji.category.smileys", entries: emojiEntries(
		[]string{"😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "😊", "😇", "🙂", "🙃", "😉", "😌", "😍", "🥰", "😘", "😗", "😙", "😚", "😋", "😛", "😝", "😜", "🤪", "🤨", "🧐", "🤓", "😎", "🥳", "😏", "😒", "😞", "😔", "😟", "😕", "🙁", "☹️", "😣", "😖", "😫", "😩", "🥺", "😢", "😭", "😤", "😠", "😡", "🤬", "🤯", "😳", "🥵", "🥶", "😱", "😨", "😰", "😥", "😓", "🤗", "🤔", "🤭", "🤫", "🤥", "😶", "😐", "😑"},
		"face smile смайл лицо улыбка",
	)},
	{id: emojiCategoryGestures, nameKey: "emoji.category.gestures", entries: emojiEntries(
		[]string{"👋", "🤚", "🖐️", "✋", "🖖", "👌", "🤏", "✌️", "🤞", "🤟", "🤘", "🤙", "👈", "👉", "👆", "👇", "☝️", "👍", "👎", "✊", "👊", "🤛", "🤜", "👏", "🙌", "👐", "🤲", "🤝", "🙏", "💪"},
		"gesture hand жест рука",
	)},
	{id: emojiCategoryAnimals, nameKey: "emoji.category.animals", entries: emojiEntries(
		[]string{"🐶", "🐱", "🐭", "🐹", "🐰", "🦊", "🐻", "🐼", "🐨", "🐯", "🦁", "🐮", "🐷", "🐸", "🐵", "🙈", "🙉", "🙊", "🐔", "🐧", "🐦", "🐤", "🦄", "🐝", "🦋", "🐌", "🐞"},
		"animal pet животное питомец",
	)},
	{id: emojiCategoryFood, nameKey: "emoji.category.food", entries: emojiEntries(
		[]string{"🍏", "🍎", "🍐", "🍊", "🍋", "🍌", "🍉", "🍇", "🍓", "🫐", "🍈", "🍒", "🍑", "🥭", "🍍", "🥝", "🍅", "🥑", "🥦", "🥕", "🌽", "🌶️", "🍄", "🍞", "🥐", "🧀", "🍕", "🍔", "🍟", "🍣", "🍿", "☕", "🍺"},
		"food drink еда напиток",
	)},
	{id: emojiCategoryTravel, nameKey: "emoji.category.travel", entries: emojiEntries(
		[]string{"🚗", "🚕", "🚌", "🚎", "🏎️", "🚓", "🚑", "🚒", "🚐", "🛻", "🚚", "🚲", "🛴", "✈️", "🚀", "🚁", "🚂", "🚆", "🚇", "🚢", "⛵", "🗺️"},
		"travel vehicle trip транспорт путешествие",
	)},
	{id: emojiCategoryActivities, nameKey: "emoji.category.activities", entries: emojiEntries(
		[]string{"⚽", "🏀", "🏈", "⚾", "🎾", "🏐", "🏉", "🎱", "🏓", "🏸", "🥅", "⛳", "🏹", "🎣", "🥊", "🎮", "🎲", "🎯", "🎨", "🎭", "🎉", "🎊", "🔥", "💡"},
		"activity sport game праздник спорт игра",
	)},
	{id: emojiCategorySymbols, nameKey: "emoji.category.symbols", entries: emojiEntries(
		[]string{"❤️", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎", "💔", "💕", "💖", "💗", "❣️", "💞", "💓", "💘", "💝", "💟", "💰", "⭐", "☀️", "✅", "☮️", "✝️", "☪️", "🕉️", "☸️", "✡️", "🔯", "🕎", "☯️", "☦️", "🛐", "⛎"},
		"symbol знак символ",
	)},
	{id: emojiCategoryFlags, nameKey: "emoji.category.flags", entries: emojiEntries(
		[]string{"🏁", "🚩", "🎌", "🏴", "🏳️", "🏳️‍🌈", "🇺🇳", "🇬🇧", "🇺🇸", "🇪🇸", "🇫🇷", "🇩🇪", "🇮🇹", "🇷🇺", "🇺🇦", "🇨🇳", "🇯🇵", "🇰🇷", "🇮🇳", "🇧🇷", "🇬🇷"},
		"flag country флаг страна",
	)},
}

var emojiSpecificKeywords = map[string]string{
	// Smileys.
	"😀": "grinning happy smile улыбка радость", "😃": "grinning big smile широкая улыбка", "😄": "grinning smiling eyes счастливая улыбка",
	"😁": "beaming grin сияющая улыбка", "😆": "squinting laugh смех зажмурился", "😅": "sweat smile relief пот улыбка облегчение",
	"😂": "laugh tears joy смех слезы радость", "🤣": "rolling laugh floor хохот смех", "😊": "blush smile happy румянец улыбка",
	"😇": "angel halo innocent ангел нимб невинный", "🙂": "slight smile легкая улыбка", "🙃": "upside down face перевернутое лицо",
	"😉": "wink playful подмигивание игривый", "😌": "relieved peaceful облегчение спокойствие", "😍": "heart eyes love сердце любовь восхищение",
	"🥰": "hearts affection love сердце нежность любовь", "😘": "kiss heart love поцелуй сердце любовь", "😗": "kissing face поцелуй целует",
	"😙": "kissing smiling eyes поцелуй улыбка", "😚": "kissing closed eyes поцелуй закрытые глаза", "😋": "tasty delicious yum вкусно облизывается",
	"😛": "tongue playful язык игривый", "😝": "squinting tongue язык зажмурился", "😜": "wink tongue язык подмигивание",
	"🤪": "zany crazy goofy безумный дурачится", "🤨": "raised eyebrow skeptical поднятая бровь скептик", "🧐": "monocle curious монокль любопытство",
	"🤓": "nerd glasses geek ботан очки умник", "😎": "cool shades glasses крутой темные очки", "🥳": "party face celebration вечеринка праздник",
	"😏": "smirk smug усмешка самодовольный", "😒": "unamused annoyed недовольный раздражение", "😞": "disappointed sad разочарование грусть",
	"😔": "pensive sad задумчивый грусть", "😟": "worried concern беспокойство тревога", "😕": "confused puzzled растерянный непонимание",
	"🙁": "slight frown sad нахмурился грусть", "☹️": "frowning sad хмурое лицо грусть", "😣": "persevering struggle упорство тяжело",
	"😖": "confounded frustrated ошеломление досада", "😫": "tired exhausted усталость измотанный", "😩": "weary exhausted измученный усталость",
	"🥺": "pleading puppy eyes умоляет просит", "😢": "cry sad tear плач грусть слеза", "😭": "sobbing cry tears рыдание плач слезы",
	"😤": "triumph steam триумф пар из носа", "😠": "angry mad злой сердитый", "😡": "rage angry ярость злость",
	"🤬": "swearing cursing symbols ругается мат цензура", "🤯": "mind blown explosion шок взрыв мозга", "😳": "flushed embarrassed смущение покраснел",
	"🥵": "hot sweating жарко перегрев", "🥶": "cold freezing холодно замерз", "😱": "scream fear крик ужас страх",
	"😨": "fearful scared испуг страх", "😰": "anxious sweat тревога холодный пот", "😥": "sad relieved sweat грусть облегчение",
	"😓": "downcast sweat пот подавленный", "🤗": "hug embrace объятие обнимает", "🤔": "thinking ponder думает размышляет",
	"🤭": "hand over mouth giggle рука у рта хихикает", "🤫": "quiet shush secret тишина ссс секрет", "🤥": "lying pinocchio nose ложь врет нос",
	"😶": "silent no mouth молчание без рта", "😐": "neutral expression нейтральный без эмоций", "😑": "expressionless blank безразличный пустой взгляд",

	// Gestures.
	"👋": "waving hello goodbye машет привет пока", "🤚": "raised back hand поднятая тыльная ладонь", "🖐️": "spread fingers hand раскрытая ладонь пальцы",
	"✋": "raised hand stop поднятая рука стоп", "🖖": "vulcan salute spock вулканское приветствие спок", "👌": "ok perfect окей хорошо",
	"🤏": "pinching small little щепотка маленький", "✌️": "victory peace two победа мир два", "🤞": "crossed fingers luck скрестил пальцы удача",
	"🤟": "love you gesture люблю жест", "🤘": "rock horns metal рок коза металл", "🤙": "call me shaka позвони шака",
	"👈": "point left указывает влево", "👉": "point right указывает вправо", "👆": "point up указывает вверх",
	"👇": "point down указывает вниз", "☝️": "index finger up указательный палец вверх", "👍": "thumb up like approve палец вверх лайк",
	"👎": "thumb down dislike reject палец вниз дизлайк", "✊": "raised fist solidarity кулак солидарность", "👊": "fist bump punch кулак удар",
	"🤛": "left fist bump кулак влево", "🤜": "right fist bump кулак вправо", "👏": "clap applause хлопает аплодисменты",
	"🙌": "raised hands hooray руки вверх ура", "👐": "open hands открытые ладони", "🤲": "palms up together ладони вместе вверх",
	"🤝": "handshake agreement рукопожатие соглашение", "🙏": "pray thanks please молитва спасибо пожалуйста", "💪": "biceps strong muscle бицепс сила мышца",

	// Animals.
	"🐶": "dog puppy собака щенок", "🐱": "cat kitten кошка котенок", "🐭": "mouse мышь мышонок",
	"🐹": "hamster хомяк", "🐰": "rabbit bunny кролик заяц", "🦊": "fox лиса лисица",
	"🐻": "bear медведь", "🐼": "panda панда", "🐨": "koala коала",
	"🐯": "tiger тигр", "🦁": "lion лев", "🐮": "cow корова",
	"🐷": "pig свинья поросенок", "🐸": "frog лягушка", "🐵": "monkey обезьяна",
	"🙈": "see no evil monkey обезьяна не вижу", "🙉": "hear no evil monkey обезьяна не слышу", "🙊": "speak no evil monkey обезьяна молчу",
	"🐔": "chicken hen курица", "🐧": "penguin пингвин", "🐦": "bird птица",
	"🐤": "chick baby bird цыпленок птенец", "🦄": "unicorn единорог", "🐝": "bee honeybee пчела",
	"🦋": "butterfly бабочка", "🐌": "snail улитка", "🐞": "ladybug beetle божья коровка жук",

	// Food and drink.
	"🍏": "green apple fruit зеленое яблоко фрукт", "🍎": "red apple fruit красное яблоко фрукт", "🍐": "pear fruit груша фрукт",
	"🍊": "orange tangerine fruit апельсин мандарин", "🍋": "lemon citrus лимон цитрус", "🍌": "banana fruit банан фрукт",
	"🍉": "watermelon fruit арбуз", "🍇": "grapes fruit виноград", "🍓": "strawberry berry клубника ягода",
	"🫐": "blueberries berry черника голубика ягода", "🍈": "melon fruit дыня", "🍒": "cherries berry вишня черешня",
	"🍑": "peach fruit персик", "🥭": "mango fruit манго", "🍍": "pineapple fruit ананас",
	"🥝": "kiwi fruit киви фрукт", "🍅": "tomato vegetable помидор томат", "🥑": "avocado fruit авокадо",
	"🥦": "broccoli vegetable брокколи овощ", "🥕": "carrot vegetable морковь овощ", "🌽": "corn maize кукуруза",
	"🌶️": "hot pepper chili острый перец чили", "🍄": "mushroom гриб", "🍞": "bread loaf хлеб буханка",
	"🥐": "croissant pastry круассан выпечка", "🧀": "cheese wedge сыр", "🍕": "pizza slice пицца",
	"🍔": "hamburger burger гамбургер бургер", "🍟": "french fries chips картофель фри", "🍣": "sushi суши роллы",
	"🍿": "popcorn cinema попкорн кино", "☕": "coffee hot drink кофе горячий напиток", "🍺": "beer mug pint пиво кружка",

	// Travel and transport.
	"🚗": "car automobile машина автомобиль", "🚕": "taxi cab такси", "🚌": "bus автобус",
	"🚎": "trolleybus bus троллейбус", "🏎️": "racing car formula гоночная машина формула", "🚓": "police car полицейская машина",
	"🚑": "ambulance скорая помощь", "🚒": "fire engine truck пожарная машина", "🚐": "minibus van микроавтобус фургон",
	"🛻": "pickup truck пикап", "🚚": "delivery truck lorry грузовик доставка", "🚲": "bicycle bike велосипед",
	"🛴": "scooter kick самокат", "✈️": "airplane plane flight самолет полет", "🚀": "rocket space ракета космос",
	"🚁": "helicopter вертолет", "🚂": "locomotive train паровоз поезд", "🚆": "train railway поезд железная дорога",
	"🚇": "metro subway метро подземка", "🚢": "ship vessel корабль судно", "⛵": "sailboat yacht парусник яхта",
	"🗺️": "world map geography карта мира география",

	// Activities.
	"⚽": "football soccer ball футбол мяч", "🏀": "basketball ball баскетбол мяч", "🏈": "american football ball американский футбол",
	"⚾": "baseball ball бейсбол мяч", "🎾": "tennis ball теннис мяч", "🏐": "volleyball ball волейбол мяч",
	"🏉": "rugby ball регби мяч", "🎱": "billiards pool eight ball бильярд шар", "🏓": "table tennis ping pong настольный теннис пинг понг",
	"🏸": "badminton racket бадминтон ракетка", "🥅": "goal net ворота сетка гол", "⛳": "golf hole flag гольф лунка",
	"🏹": "bow arrow archery лук стрела стрельба", "🎣": "fishing rod fish рыбалка удочка", "🥊": "boxing glove бокс перчатка",
	"🎮": "video game controller видеоигра геймпад джойстик", "🎲": "dice game random кубик игра случайность", "🎯": "target bullseye darts мишень цель дартс",
	"🎨": "artist palette paint художник палитра краски", "🎭": "theater masks drama театр маски драма", "🎉": "party popper celebration вечеринка праздник хлопушка",
	"🎊": "confetti celebration конфетти праздник", "🔥": "fire flame hot огонь пламя жар", "💡": "light bulb idea лампочка идея свет",

	// Symbols.
	"❤️": "red heart love красное сердце любовь", "💛": "yellow heart желтое сердце", "💚": "green heart зеленое сердце",
	"💙": "blue heart синее сердце", "💜": "purple heart фиолетовое сердце", "🖤": "black heart черное сердце",
	"🤍": "white heart белое сердце", "🤎": "brown heart коричневое сердце", "💔": "broken heart разбитое сердце",
	"💕": "two hearts love два сердца любовь", "💖": "sparkling heart сверкающее сердце", "💗": "growing heart растущее сердце",
	"❣️": "heart exclamation сердце восклицание", "💞": "revolving hearts вращающиеся сердца", "💓": "beating heart бьющееся сердце",
	"💘": "heart arrow cupid сердце стрела купидон", "💝": "heart ribbon gift сердце лента подарок", "💟": "heart decoration сердце украшение",
	"💰": "money bag cash деньги мешок наличные", "⭐": "star favorite звезда избранное", "☀️": "sun sunshine солнце солнечный",
	"✅": "check mark tick галочка отметка готово", "☮️": "peace symbol мир пацифик", "✝️": "latin cross christian латинский крест христианство",
	"☪️": "star crescent islam полумесяц ислам", "🕉️": "om hinduism ом индуизм", "☸️": "dharma wheel buddhism дхарма колесо буддизм",
	"✡️": "david judaism hexagram давид иудаизм гексаграмма", "🔯": "dotted six pointed hexagram точки шестиконечник гексаграмма", "🕎": "menorah judaism менора иудаизм",
	"☯️": "yin yang tao инь ян дао", "☦️": "orthodox cross christian православный крест", "🛐": "place worship prayer место молитвы религия",
	"⛎": "ophiuchus zodiac змееносец зодиак",

	// Flags.
	"🏁": "chequered racing finish flag клетчатый флаг финиш гонка", "🚩": "triangular red flag треугольный красный флаг", "🎌": "crossed japanese flags скрещенные японские флаги",
	"🏴": "black flag черный флаг", "🏳️": "white flag surrender белый флаг капитуляция", "🏳️‍🌈": "rainbow pride flag радужный флаг прайд",
	"🇺🇳": "united nations un flag оон объединенные нации флаг", "🇬🇧": "united kingdom britain uk flag великобритания британия флаг", "🇺🇸": "united states america usa flag сша америка флаг",
	"🇪🇸": "spain spanish flag испания испанский флаг", "🇫🇷": "france french flag франция французский флаг", "🇩🇪": "germany german flag германия немецкий флаг",
	"🇮🇹": "italy italian flag италия итальянский флаг", "🇷🇺": "russia russian flag россия русский флаг", "🇺🇦": "ukraine ukrainian flag украина украинский флаг",
	"🇨🇳": "china chinese flag китай китайский флаг", "🇯🇵": "japan japanese flag япония японский флаг", "🇰🇷": "south korea korean flag южная корея корейский флаг",
	"🇮🇳": "india indian flag индия индийский флаг", "🇧🇷": "brazil brazilian flag бразилия бразильский флаг", "🇬🇷": "greece greek flag греция греческий флаг",
}

func emojiEntries(values []string, categoryKeywords string) []emojiEntry {
	entries := make([]emojiEntry, 0, len(values))
	categoryTokens := emojiSearchTokens(categoryKeywords)
	for _, value := range values {
		entries = append(entries, emojiEntry{
			value:        value,
			searchTokens: mergeEmojiSearchTokens(categoryTokens, emojiSearchTokens(emojiSpecificKeywords[value])),
		})
	}
	return entries
}

// mergeEmojiSearchTokens joins a category's tokens with one emoji's own and
// drops the repeats: "flag" is in the flags blob and in every flag's name,
// "поцелуй" in three kissing faces at once. A repeated token cannot widen what
// emojiMatchesQuery accepts — the prefix scan stops at the first hit — so it
// only ever costs the scan that finds it a second time.
func mergeEmojiSearchTokens(categoryTokens, nameTokens []string) []string {
	merged := make([]string, 0, len(categoryTokens)+len(nameTokens))
	seen := make(map[string]struct{}, len(categoryTokens)+len(nameTokens))
	for _, tokens := range [][]string{categoryTokens, nameTokens} {
		for _, token := range tokens {
			if _, known := seen[token]; known {
				continue
			}
			seen[token] = struct{}{}
			merged = append(merged, token)
		}
	}
	return merged
}

// emojiCategoryValues holds each category's emoji in draw order, built once
// from the catalog. The unsearched grid is the common case and it re-reads
// this list every frame, which is no reason to rebuild it every frame.
var emojiCategoryValues = buildEmojiCategoryValues()

func buildEmojiCategoryValues() map[emojiCategoryID][]string {
	byCategory := make(map[emojiCategoryID][]string, len(emojiCategories))
	for _, category := range emojiCategories {
		values := make([]string, 0, len(category.entries))
		for _, entry := range category.entries {
			values = append(values, entry.value)
		}
		byCategory[category.id] = values
	}
	return byCategory
}

// emojiValues is the catalog's own slice, not a copy, and callers must treat
// it as READ-ONLY. Each is built at its exact length, so a caller that appends
// to it gets a copy of its own rather than writing into the catalog; a caller
// that assigns through an index would corrupt it for the whole process, and
// none does.
func emojiValues(categoryID emojiCategoryID) []string {
	return emojiCategoryValues[categoryID]
}

func filterEmojiChoices(categoryID emojiCategoryID, query string, recents []string) []string {
	normalizedQuery := strings.ToLower(strings.TrimSpace(query))
	if normalizedQuery == "" {
		if categoryID == emojiCategoryRecent {
			return append([]string(nil), recents...)
		}
		return emojiValues(categoryID)
	}

	queryTokens := emojiSearchTokens(normalizedQuery)
	matches := make([]string, 0, 16)
	for _, category := range emojiCategories {
		for _, entry := range category.entries {
			if emojiMatchesQuery(entry.value, entry.searchTokens, normalizedQuery, queryTokens) {
				matches = append(matches, entry.value)
			}
		}
	}
	if len(matches) == 0 {
		return nil
	}
	return matches
}

func emojiMatchesQuery(value string, keywordTokens []string, normalizedQuery string, queryTokens []string) bool {
	if strings.Contains(value, normalizedQuery) {
		return true
	}
	if len(queryTokens) == 0 {
		return false
	}
	for _, queryToken := range queryTokens {
		matched := false
		for _, keywordToken := range keywordTokens {
			if strings.HasPrefix(keywordToken, queryToken) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func emojiSearchTokens(value string) []string {
	return strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
}

func rememberRecentEmoji(recents []string, selected string) []string {
	next := make([]string, 0, min(maxRecentEmojis, len(recents)+1))
	next = append(next, selected)
	for _, recent := range recents {
		if recent == selected {
			continue
		}
		next = append(next, recent)
		if len(next) == maxRecentEmojis {
			break
		}
	}
	return next
}

func normalizeRecentEmojis(values []string) []string {
	normalized := make([]string, 0, min(maxRecentEmojis, len(values)))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if !isKnownEmoji(value) {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
		if len(normalized) == maxRecentEmojis {
			break
		}
	}
	return normalized
}

func isKnownEmoji(value string) bool {
	for _, category := range emojiCategories {
		for _, entry := range category.entries {
			if entry.value == value {
				return true
			}
		}
	}
	return false
}

func insertEmoji(editor *widget.Editor, selected string) {
	if editor == nil || selected == "" {
		return
	}
	editor.Insert(selected)
}

// emojiPickerState is the application's half of the picker: whether it is
// showing, the toggle that opens it, the recents it persists and the keyboard
// state it restores on close. The panel's own state — category, query, scroll
// positions and every widget the user touches — lives in the component.
//
// The two are kept apart rather than merged because they answer to different
// owners. The component's half is reset by the component; this half is written
// from the touch-keyboard subsystem, from preferences and from the window's
// navigation stack, and none of those has any business reaching into a widget.
type emojiPickerState struct {
	panel        ui.EmojiPickerState
	visible      bool
	toggleButton widget.Clickable
	recents      []string

	restoreSoftKeyboard          bool
	restorePlatformTouchKeyboard bool
	suppressSoftKeyboardOnOpen   bool
	recentSavePending            bool
	recentSaveAt                 time.Time
}

func newEmojiPickerState() emojiPickerState {
	return emojiPickerState{panel: ui.NewEmojiPickerState(string(emojiCategorySmileys))}
}

func newEmojiPickerStateWithRecents(recents []string) emojiPickerState {
	state := newEmojiPickerState()
	state.recents = normalizeRecentEmojis(recents)
	return state
}

// query is the text currently in the composer panel's search field.
func (state *emojiPickerState) query() string {
	return state.panel.Search.Text()
}

// choices refreshes the composer panel's grid for this frame. See
// ui.EmojiPickerState.Choices for why a disabled source reuses the last set.
func (state *emojiPickerState) choices(sourceEnabled bool) []string {
	return emojiChoicesFor(&state.panel, sourceEnabled, state.recents)
}

// emojiChoicesFor is the catalogue lookup behind any panel's grid. The recents
// are the window's one list, shared by both panels: which emoji a person
// reaches for does not depend on whether they are writing or reacting.
func emojiChoicesFor(panel *ui.EmojiPickerState, sourceEnabled bool, recents []string) []string {
	return panel.Choices(sourceEnabled, func(category, query string) []string {
		return filterEmojiChoices(emojiCategoryID(category), query, recents)
	})
}

func (w *Window) handleEmojiActions(gtx layout.Context) {
	w.flushRecentEmojiPreferences(gtx.Now, false)
	if w.emojiPicker.recentSavePending {
		// A newer tap may have moved the trailing-edge deadline beyond an
		// already queued wake-up. Re-arm it on that early frame so persistence
		// does not have to wait for the unrelated periodic UI heartbeat.
		gtx.Execute(op.InvalidateCmd{At: w.emojiPicker.recentSaveAt})
	}
	for w.emojiPicker.toggleButton.Clicked(gtx) {
		if w.emojiPicker.visible {
			w.closeEmojiPicker(gtx)
		} else {
			w.openEmojiPicker(gtx)
		}
	}
	if !w.emojiPicker.visible {
		return
	}

	for {
		categoryID, ok := w.emojiPicker.panel.CategoryClicked(gtx)
		if !ok {
			break
		}
		w.emojiPicker.panel.SelectCategory(categoryID)
	}
	for {
		value, ok := w.emojiPicker.panel.Clicked(gtx)
		if !ok {
			break
		}
		w.selectEmoji(gtx, value)
	}
}

func (w *Window) openEmojiPicker(gtx layout.Context) {
	if w.emojiPicker.visible {
		return
	}
	w.emojiPicker.visible = true
	platformKeyboardVisible := w.touchKbd.shownByUs.Load() ||
		w.touchKbd.paneVisible.Load() ||
		w.touchKbd.occludedDp.Load() > 0
	w.emojiPicker.restorePlatformTouchKeyboard = w.touchKbd.platformTouchKeyboardExpected.Load() || platformKeyboardVisible
	w.emojiPicker.restoreSoftKeyboard = w.touchKbd.softKeyboardExpected.Load() || w.emojiPicker.restorePlatformTouchKeyboard
	w.emojiPicker.suppressSoftKeyboardOnOpen = true
	// The selection survives a close; where the row was scrolled to does not
	// have to, and on a narrow picker the two disagree — reopening on the
	// flags category used to show a row that highlighted nothing at all.
	w.emojiPicker.panel.RevealCategory()
	gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
	// Keep logical focus and the caret in the composer, but dismiss the
	// on-screen keyboard while the emoji surface supplies input instead.
	// The explicit cancel also prevents a Windows show request that is still
	// settling from raising the keyboard over the picker afterwards.
	w.touchKbd.cancelPendingShow()
	gtx.Execute(key.SoftKeyboardCmd{Show: false})
	requestTouchKeyboardHide(&w.touchKbd)
}

func (w *Window) closeEmojiPicker(gtx layout.Context) {
	if !w.emojiPicker.visible {
		return
	}
	w.emojiPicker.visible = false
	w.emojiPicker.suppressSoftKeyboardOnOpen = false
	w.emojiPicker.panel.ResetSearch()
	gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
	restoreSoftKeyboard := w.emojiPicker.restoreSoftKeyboard
	restorePlatformTouchKeyboard := w.emojiPicker.restorePlatformTouchKeyboard
	w.emojiPicker.restoreSoftKeyboard = false
	w.emojiPicker.restorePlatformTouchKeyboard = false
	if restoreSoftKeyboard {
		gtx.Execute(key.SoftKeyboardCmd{Show: true})
		w.touchKbd.softKeyboardExpected.Store(true)
	}
	if restorePlatformTouchKeyboard {
		showTouchKeyboard(&w.touchKbd)
	}
}

func (state *emojiPickerState) takeSoftKeyboardSuppression(sourceEnabled bool) bool {
	if !sourceEnabled || !state.visible || !state.suppressSoftKeyboardOnOpen {
		return false
	}
	state.suppressSoftKeyboardOnOpen = false
	return true
}

func (w *Window) selectEmoji(gtx layout.Context, value string) {
	insertEmoji(&w.messageEditor, value)
	w.rememberEmoji(gtx, value)
	gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
}

// rememberEmoji moves one emoji to the head of the recents list and schedules
// the write. Split out of selectEmoji because "insert it in the draft" is what
// the COMPOSER does with a choice, and the panel now serves a second caller.
func (w *Window) rememberEmoji(gtx layout.Context, value string) {
	w.rememberEmojiAt(value, gtx.Now)
	if w.emojiPicker.recentSavePending {
		gtx.Execute(op.InvalidateCmd{At: w.emojiPicker.recentSaveAt})
	}
}

// rememberEmojiAt is the same without a frame in hand.
//
// The reaction surfaces choose emoji outside a layout callback — a tap is
// handled, the decision goes to the service, and there is no gtx to schedule the
// trailing-edge save with. It does not need one: handleEmojiActions re-arms the
// wake-up on the next frame while a save is pending, and a tap always produces
// one.
func (w *Window) rememberEmojiAt(value string, now time.Time) {
	w.emojiPicker.recents = rememberRecentEmoji(w.emojiPicker.recents, value)
	if w.prefs == nil {
		return
	}
	w.prefs.RecentEmojis = append([]string(nil), w.emojiPicker.recents...)
	w.emojiPicker.recentSavePending = true
	w.emojiPicker.recentSaveAt = now.Add(emojiRecentSaveDelay)
}

func (w *Window) flushRecentEmojiPreferences(now time.Time, force bool) {
	if !w.emojiPicker.recentSavePending || w.prefs == nil {
		return
	}
	if !force && now.Before(w.emojiPicker.recentSaveAt) {
		return
	}
	w.emojiPicker.recentSavePending = false
	if err := w.prefs.Save(); err != nil {
		log.Warn().Err(err).Msg("save recent emojis")
	}
}

func (w *Window) handleEmojiEscapeNavigation(gtx layout.Context) {
	if w.topNavigationDismissTarget(gtx) != dismissEmojiPicker {
		return
	}
	for {
		ev, ok := gtx.Event(key.Filter{Name: key.NameEscape})
		if !ok {
			return
		}
		ke, ok := ev.(key.Event)
		if ok && ke.State == key.Press {
			w.closeEmojiPicker(gtx)
			w.dropEmojiToggleClicks(gtx)
		}
	}
}

// dropEmojiToggleClicks discards toggle presses queued for THIS frame.
//
// Dismissal runs before handleEmojiActions reads the widget, so a tap on the
// toggle delivered in the same frame as Escape or Back would re-open the
// picker the key had just closed. Reordering the two handlers is not the fix:
// with clicks read first, the tap closes the picker and the key then falls
// through to the surface beneath it, which in the compact layout means Escape
// leaves the conversation. One gesture, one outcome — and the discarded tap
// asked for the same outcome the key already produced.
func (w *Window) dropEmojiToggleClicks(gtx layout.Context) {
	for w.emojiPicker.toggleButton.Clicked(gtx) {
	}
}

// emojiPickerRoom is the height the composer can give the open picker this
// frame, and 0 when there is not enough of it for the surface to be worth
// drawing. On 0 the picker stays OPEN — the draw is deferred, not cancelled —
// and the touch keyboard is asked to get out of the way, which is what usually
// frees the room: opening the picker dismisses the keyboard, but a later tap
// in the composer is allowed to bring it back up over the picker.
//
// Nothing is asked for during the measuring pass keyboardTailRow runs the
// composer through. That pass hands the row the occlusion back, so it measures
// a window with the keyboard already gone and its answer says nothing about
// what is on screen now; gtx.Enabled() is how it is recognised, as in
// ui.EmojiPickerState.Choices.
//
// A deferred picker keeps consuming Escape and Back, deliberately: it is still
// open, its toggle still reads "close", and both keys are handled in
// handleActions outside any layout — so the way out stays live even on a
// window that can never draw the surface. That is the same bargain
// menuOverlayRoom makes, and it is what the LIMIT on requestTouchKeyboardRoom
// requires of every caller.
func (w *Window) emojiPickerRoom(gtx layout.Context, chromeHeight, editorHeight, footerReserve int) int {
	height := composerPickerHeight(
		gtx.Constraints.Max.Y,
		chromeHeight,
		editorHeight,
		footerReserve,
		ui.EmojiPickerMinHeight(gtx, ui.EmojiPickerModeCompose),
		gtx.Dp(ui.EmojiPickerDesiredHeightDp),
	)
	if !gtx.Enabled() {
		return height
	}
	if height > 0 {
		w.emojiKbdHideAskedGen = 0
		return height
	}
	requestTouchKeyboardRoom(&w.touchKbd, &w.emojiKbdHideAskedGen)
	return 0
}

// emojiCategoryOrder is the order the category chips are drawn in: recent
// first, then the catalog's own order.
var emojiCategoryOrder = []emojiCategoryID{
	emojiCategoryRecent,
	emojiCategorySmileys,
	emojiCategoryGestures,
	emojiCategoryAnimals,
	emojiCategoryFood,
	emojiCategoryTravel,
	emojiCategoryActivities,
	emojiCategorySymbols,
	emojiCategoryFlags,
}

// emojiPickerCategories describes the chip row to the component: the catalogue's
// order, this window's icons and the labels in this window's language.
func (w *Window) emojiPickerCategories() []ui.EmojiPickerCategory {
	categories := make([]ui.EmojiPickerCategory, 0, len(emojiCategoryOrder))
	for _, categoryID := range emojiCategoryOrder {
		categories = append(categories, ui.EmojiPickerCategory{
			ID:   string(categoryID),
			Icon: w.emojiCategoryIcons[categoryID],
			Hint: w.t(emojiCategoryNameKey(categoryID)),
		})
	}
	return categories
}

// emojiPickerSelection is the chip a panel should highlight: none while a
// global query is running, since the grid then shows matches from every
// category and no chip describes what is on screen.
//
// The component keys its chips by plain strings — it draws whatever list it is
// handed and has no catalogue to type them against — so the conversion back to
// the catalogue's own type happens here and in emojiChoicesFor, at the two
// boundaries, rather than leaking untyped identifiers further in.
func emojiPickerSelection(panel *ui.EmojiPickerState) string {
	category := emojiCategoryID(panel.Category())
	if !emojiCategoryIsActive(category, category, panel.Search.Text()) {
		return ""
	}
	return string(category)
}

// emojiPickerDescriptor builds the panel description shared by both modes.
// mode selects the header and the wording; everything else is common.
func (w *Window) emojiPickerDescriptor(panel *ui.EmojiPickerState, mode ui.EmojiPickerMode, choices []string) ui.EmojiPicker {
	return ui.EmojiPicker{
		Mode:       mode,
		Categories: w.emojiPickerCategories(),
		Selected:   emojiPickerSelection(panel),
		Choices:    choices,
		Labels: ui.EmojiPickerLabels{
			SearchPlaceholder: w.t("emoji.search_placeholder"),
			Empty:             w.t("emoji.empty"),
			Title:             w.t("reaction.pick"),
			CloseHint:         w.t("reaction.pick_close"),
			Describe: func(value string) string {
				if mode == ui.EmojiPickerModeReaction {
					return w.t("reaction.apply", value)
				}
				return w.t("emoji.insert", value)
			},
		},
		SearchWrap: func(gtx layout.Context, editor layout.Widget) layout.Dimensions {
			// The query is the user's own text — an English word searched from
			// the Arabic interface, an Arabic one from the English build — so
			// it takes its direction from itself and not from the language
			// around it. The picker hands its search editor to this wrapper
			// before laying it out, which makes this the last point before the
			// editor turns key presses into caret movement.
			gtx = directedByContent(gtx, panel.Search.Text())
			return editorTouchKeyboardArea(gtx, &w.touchKbdTags[3], &w.touchKbd, editor)
		},
		SearchIcon: w.searchIcon,
	}
}

func (w *Window) layoutEmojiPicker(gtx layout.Context) layout.Dimensions {
	choices := w.emojiPicker.choices(gtx.Enabled())
	return w.kit().EmojiPicker(gtx, &w.emojiPicker.panel,
		w.emojiPickerDescriptor(&w.emojiPicker.panel, ui.EmojiPickerModeCompose, choices))
}

func emojiCategoryIsActive(selected, category emojiCategoryID, query string) bool {
	return strings.TrimSpace(query) == "" && selected == category
}

func emojiCategoryNameKey(categoryID emojiCategoryID) string {
	if categoryID == emojiCategoryRecent {
		return "emoji.category.recent"
	}
	for _, category := range emojiCategories {
		if category.id == categoryID {
			return category.nameKey
		}
	}
	return "emoji.category.smileys"
}
