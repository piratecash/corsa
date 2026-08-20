package desktop

import (
	"image"
	"image/color"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/rs/zerolog/log"

	"gioui.org/font"
	"gioui.org/io/key"
	"gioui.org/io/semantic"
	"gioui.org/layout"
	"gioui.org/op"
	"gioui.org/op/clip"
	"gioui.org/op/paint"
	"gioui.org/unit"
	"gioui.org/widget"
	"gioui.org/widget/material"
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

// The width one emoji cell aims for; the grid fits as many columns as that
// allows, never fewer than four.
const emojiGridCellWidthDp = unit.Dp(52)

// The picker's vertical budget, in one place. Every constant here is used at
// the single site that draws the thing it measures, so the sums below cannot
// drift away from the surface they describe.
const (
	emojiPickerBorderDp      = unit.Dp(1) // frame around the whole surface
	emojiPickerPaddingDp     = unit.Dp(6) // inset between frame and content
	emojiPickerSpacingDp     = unit.Dp(6) // categories → search → grid
	emojiCategoryIconDp      = unit.Dp(17)
	emojiCategoryIconInsetDp = unit.Dp(5)
	emojiSearchHeightDp      = unit.Dp(34)
	emojiGridCellHeightDp    = unit.Dp(38)

	// How tall the picker opens when the composer can afford it.
	emojiPickerDesiredHeightDp = unit.Dp(250)
)

// emojiGlyphSizeSp is the type size one emoji is drawn at in the grid. Its ink
// is a square of exactly this height — an emoji face's ascent is one em — so
// it also fixes how much of emojiGridCellHeightDp the glyph fills.
const emojiGlyphSizeSp = unit.Sp(22)

// emojiCategoryChipPx is how wide and tall a full-size category chip draws,
// summed from its PARTS the way the icon button draws them: at 1.5 px/dp the
// 17dp icon rounds to 26px and each 5dp inset to 8px, one pixel more than
// rounding the 27dp total would claim.
func emojiCategoryChipPx(gtx layout.Context) int {
	return gtx.Dp(emojiCategoryIconDp) + 2*gtx.Dp(emojiCategoryIconInsetDp)
}

// emojiPickerChromeHeight is everything the picker draws around its grid: the
// frame, the content insets, the category row, the search field and the two
// spacers between them. Every term carries its own draw site's rounding and
// they are summed in PIXELS, rather than taking gtx.Dp of a total in dp — the
// two differ by up to a pixel per component at fractional densities, and this
// number is compared against real pixels.
//
// The category row is counted at FULL chip height and always comes out that
// tall: a row too narrow for nine chips scrolls them rather than shrinking
// them (layoutEmojiCategories), so this is the row's real height at every
// width, not an upper bound on it.
func emojiPickerChromeHeight(gtx layout.Context) int {
	return 2*gtx.Dp(emojiPickerBorderDp) + 2*gtx.Dp(emojiPickerPaddingDp) +
		emojiCategoryChipPx(gtx) +
		2*gtx.Dp(emojiPickerSpacingDp) + gtx.Dp(emojiSearchHeightDp)
}

// emojiPickerMinHeight is the smallest height at which the surface can show a
// single emoji. Below it the picker is not drawn at all: a clipped strip with
// no reachable cell is worse than no picker, and an empty one is worse still —
// it is invisible, yet it is the top Escape/Back target and consumes the key
// that was meant for the surface underneath.
func emojiPickerMinHeight(gtx layout.Context) int {
	return emojiPickerChromeHeight(gtx) + gtx.Dp(emojiGridCellHeightDp)
}

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

type emojiPickerState struct {
	visible         bool
	category        emojiCategoryID
	toggleButton    widget.Clickable
	searchEditor    widget.Editor
	categoryButtons map[emojiCategoryID]*widget.Clickable
	emojiButtons    map[string]*widget.Clickable
	list            widget.List
	// Scroll position of the category row, used only on a picker too narrow
	// to spread nine chips across it. Plain layout.List, not material's: a
	// scrollbar's gutter would eat into a 27dp row.
	categoryList layout.List
	// A request to bring the selected category on screen, set wherever the
	// selection changes and consumed by layoutEmojiCategories on the next
	// enabled frame. It has to cross into layout because only the row knows
	// how wide it came out, and therefore which first index shows the chip
	// without leaving a gap after the last one. A picker wide enough to
	// spread every chip consumes nothing: there is nothing to reveal, and the
	// request keeps until the window is narrow enough for it to mean
	// something.
	revealCategory bool
	recents        []string
	// The emoji this frame's grid was built from, written by
	// resolveVisibleChoices during layout and read by handleEmojiActions at
	// the top of the NEXT frame. Crossing a frame is the point, not an
	// oversight: the clicks that frame delivers were aimed at the buttons the
	// previous layout drew, so the set those buttons came from is the set that
	// may answer for them. Reading a freshly filtered list instead would let a
	// query typed in between decide which taps count — a tap on an emoji the
	// search has since filtered out would be dropped, and one on the cell that
	// took its place would insert a character nobody touched.
	visibleChoices               []string
	restoreSoftKeyboard          bool
	restorePlatformTouchKeyboard bool
	suppressSoftKeyboardOnOpen   bool
	recentSavePending            bool
	recentSaveAt                 time.Time
}

func newEmojiPickerState() emojiPickerState {
	state := emojiPickerState{
		category:        emojiCategorySmileys,
		categoryButtons: make(map[emojiCategoryID]*widget.Clickable, len(emojiCategories)+1),
		emojiButtons:    make(map[string]*widget.Clickable),
		list:            widget.List{List: layout.List{Axis: layout.Vertical}},
		categoryList:    layout.List{Axis: layout.Horizontal, Alignment: layout.Middle},
	}
	state.searchEditor.SingleLine = true
	state.categoryButtons[emojiCategoryRecent] = new(widget.Clickable)
	for _, category := range emojiCategories {
		state.categoryButtons[category.id] = new(widget.Clickable)
		for _, entry := range category.entries {
			if state.emojiButtons[entry.value] == nil {
				state.emojiButtons[entry.value] = new(widget.Clickable)
			}
		}
	}
	return state
}

func newEmojiPickerStateWithRecents(recents []string) emojiPickerState {
	state := newEmojiPickerState()
	state.recents = normalizeRecentEmojis(recents)
	return state
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

	for categoryID, button := range w.emojiPicker.categoryButtons {
		for button.Clicked(gtx) {
			w.emojiPicker.selectCategory(categoryID)
		}
	}
	for _, value := range w.emojiPicker.visibleChoices {
		button := w.emojiPicker.emojiButtons[value]
		for button.Clicked(gtx) {
			w.selectEmoji(gtx, value)
		}
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
	w.emojiPicker.revealCategory = true
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
	// A query left behind reopens the picker on one cell with no category
	// highlighted — a state that reads as broken, and whose only explanation
	// is small text in a field the user is not looking at. The grid's scroll
	// offset goes with it: it indexes a result list that no longer exists.
	// The chip row's offset is NOT reset here — openEmojiPicker owns where
	// that row sits, and zeroing it would be the second owner that put the
	// selected chip out of sight.
	w.emojiPicker.searchEditor.SetText("")
	w.emojiPicker.list.Position = layout.Position{}
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

// selectCategory switches the grid to a category, puts its list back at the
// top and asks the chip row to show the chip that is now highlighted. The
// three belong together: a highlighted chip nobody can see reads as nothing
// being selected at all.
func (state *emojiPickerState) selectCategory(categoryID emojiCategoryID) {
	state.category = categoryID
	state.list.Position = layout.Position{}
	state.revealCategory = true
}

// emojiCategoryRowFirst is the chip the scrolling row should start on to show
// the selected one. It is the selected chip itself, pulled back far enough
// that the row still ends on the last chip: layout.List renders from First
// onwards and does not backfill, so ScrollTo(8) of nine chips would leave one
// chip beside an empty row.
func emojiCategoryRowFirst(selected, count, visible int) int {
	return max(0, min(selected, count-visible))
}

// resolveVisibleChoices refreshes the grid's contents and hands them to the
// caller, keeping the copy handleEmojiActions answers next frame's clicks from
// (see visibleChoices).
func (state *emojiPickerState) resolveVisibleChoices(sourceEnabled bool) []string {
	if !sourceEnabled {
		// keyboardTailRow measures the composer with a disabled source before
		// drawing it for real. The picker height is fixed by its parent, so the
		// previous result is sufficient for that inert pass and avoids doing the
		// same catalog search twice in one frame.
		return state.visibleChoices
	}
	state.visibleChoices = filterEmojiChoices(state.category, state.searchEditor.Text(), state.recents)
	return state.visibleChoices
}

func (w *Window) selectEmoji(gtx layout.Context, value string) {
	insertEmoji(&w.messageEditor, value)
	w.emojiPicker.recents = rememberRecentEmoji(w.emojiPicker.recents, value)
	if w.prefs != nil {
		w.prefs.RecentEmojis = append([]string(nil), w.emojiPicker.recents...)
		w.emojiPicker.recentSavePending = true
		w.emojiPicker.recentSaveAt = gtx.Now.Add(emojiRecentSaveDelay)
		gtx.Execute(op.InvalidateCmd{At: w.emojiPicker.recentSaveAt})
	}
	gtx.Execute(key.FocusCmd{Tag: &w.messageEditor})
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
// resolveVisibleChoices.
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
		emojiPickerMinHeight(gtx),
		gtx.Dp(emojiPickerDesiredHeightDp),
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

func (w *Window) layoutEmojiPicker(gtx layout.Context) layout.Dimensions {
	background := color.NRGBA{R: 18, G: 25, B: 34, A: 255}
	border := color.NRGBA{R: 46, G: 58, B: 75, A: 255}

	return layout.Stack{}.Layout(gtx,
		layout.Expanded(func(gtx layout.Context) layout.Dimensions {
			paint.FillShape(gtx.Ops, border, clip.UniformRRect(image.Rectangle{Max: gtx.Constraints.Min}, gtx.Dp(unit.Dp(10))).Op(gtx.Ops))
			return layout.Dimensions{Size: gtx.Constraints.Min}
		}),
		layout.Stacked(func(gtx layout.Context) layout.Dimensions {
			return layout.UniformInset(emojiPickerBorderDp).Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				fillRounded(gtx, background, unit.Dp(9))
				return layout.Inset{Top: emojiPickerPaddingDp, Bottom: emojiPickerPaddingDp, Left: unit.Dp(8), Right: unit.Dp(8)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
					return layout.Flex{Axis: layout.Vertical}.Layout(gtx,
						layout.Rigid(w.layoutEmojiCategories),
						layout.Rigid(layout.Spacer{Height: emojiPickerSpacingDp}.Layout),
						layout.Rigid(w.layoutEmojiSearch),
						layout.Rigid(layout.Spacer{Height: emojiPickerSpacingDp}.Layout),
						layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
							choices := w.emojiPicker.resolveVisibleChoices(gtx.Enabled())
							return w.layoutEmojiGrid(gtx, choices)
						}),
					)
				})
			})
		}),
	)
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

// layoutEmojiCategories spreads the chips across the row when they all fit at
// full size, and scrolls them at full size when they do not.
//
// Shrinking them to fit was the other option and it is the worse one: nine
// chips need 243dp, and a picker narrow enough to matter drives the icon down
// to 15dp at 140dp of row, 10dp at 90dp, 4dp at 40dp — no overlap, but nothing
// left to hit either. Nobody can be asked for horizontal room the way the
// keyboard can be asked for vertical room, so the row does what the grid below
// it already does: keeps its cells the size a finger needs and lets the ones
// that do not fit be scrolled to.
//
// Full size at every width is also what keeps emojiPickerChromeHeight exact —
// a row that shrank was a row shorter than the height budget reserved for it.
func (w *Window) layoutEmojiCategories(gtx layout.Context) layout.Dimensions {
	chip := emojiCategoryChipPx(gtx)
	if len(emojiCategoryOrder)*chip > gtx.Constraints.Max.X {
		if w.emojiPicker.revealCategory && gtx.Enabled() {
			// Not in the pass keyboardTailRow measures with: it would spend
			// the request on a row that is never shown (see emojiPickerRoom).
			w.emojiPicker.revealCategory = false
			w.emojiPicker.categoryList.ScrollTo(emojiCategoryRowFirst(
				slices.Index(emojiCategoryOrder, w.emojiPicker.category),
				len(emojiCategoryOrder),
				gtx.Constraints.Max.X/chip,
			))
		}
		return w.emojiPicker.categoryList.Layout(gtx, len(emojiCategoryOrder),
			func(gtx layout.Context, index int) layout.Dimensions {
				return w.layoutEmojiCategoryChip(gtx, emojiCategoryOrder[index])
			})
	}

	children := make([]layout.FlexChild, 0, len(emojiCategoryOrder))
	for _, categoryID := range emojiCategoryOrder {
		children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
			return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
				return w.layoutEmojiCategoryChip(gtx, categoryID)
			})
		}))
	}
	return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
}

func (w *Window) layoutEmojiCategoryChip(gtx layout.Context, categoryID emojiCategoryID) layout.Dimensions {
	background := color.NRGBA{R: 18, G: 25, B: 34, A: 255}
	iconColor := color.NRGBA{R: 150, G: 166, B: 188, A: 255}
	if emojiCategoryIsActive(w.emojiPicker.category, categoryID, w.emojiPicker.searchEditor.Text()) {
		background = color.NRGBA{R: 32, G: 76, B: 135, A: 255}
		iconColor = color.NRGBA{R: 222, G: 238, B: 255, A: 255}
	}
	style := material.IconButton(w.theme, w.emojiPicker.categoryButtons[categoryID], w.emojiCategoryIcons[categoryID], w.t(emojiCategoryNameKey(categoryID)))
	style.Background = background
	style.Color = iconColor
	style.Size = emojiCategoryIconDp
	style.Inset = layout.UniformInset(emojiCategoryIconInsetDp)
	return style.Layout(gtx)
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

func (w *Window) layoutEmojiSearch(gtx layout.Context) layout.Dimensions {
	height := gtx.Dp(emojiSearchHeightDp)
	gtx.Constraints.Min.Y = height
	gtx.Constraints.Max.Y = height
	fillRounded(gtx, color.NRGBA{R: 13, G: 19, B: 27, A: 255}, unit.Dp(7))
	return layout.Inset{Left: unit.Dp(9), Right: unit.Dp(9)}.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx,
			layout.Rigid(func(gtx layout.Context) layout.Dimensions {
				return layoutVectorIcon(gtx, w.searchIcon, unit.Dp(16), color.NRGBA{R: 115, G: 134, B: 160, A: 255})
			}),
			layout.Rigid(layout.Spacer{Width: unit.Dp(7)}.Layout),
			layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				editor := material.Editor(w.theme, &w.emojiPicker.searchEditor, w.t("emoji.search_placeholder"))
				editor.Color = color.NRGBA{R: 231, G: 237, B: 245, A: 255}
				editor.HintColor = color.NRGBA{R: 105, G: 121, B: 143, A: 255}
				editor.TextSize = unit.Sp(13)
				return editorTouchKeyboardArea(gtx, &w.touchKbdTags[3], &w.touchKbd, func(gtx layout.Context) layout.Dimensions {
					return layoutVerticallyCentered(gtx, editor.Layout)
				})
			}),
		)
	})
}

func (w *Window) layoutEmojiGrid(gtx layout.Context, values []string) layout.Dimensions {
	gtx.Constraints.Min.X = gtx.Constraints.Max.X
	if len(values) == 0 {
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Caption(w.theme, w.t("emoji.empty"))
			label.Color = color.NRGBA{R: 126, G: 143, B: 166, A: 255}
			return label.Layout(gtx)
		})
	}

	columns := emojiGridColumns(gtx.Constraints.Max.X, max(1, gtx.Dp(emojiGridCellWidthDp)))
	rows := (len(values) + columns - 1) / columns
	list := material.List(w.theme, &w.emojiPicker.list)
	return list.Layout(gtx, rows, func(gtx layout.Context, row int) layout.Dimensions {
		gtx.Constraints.Min.X = gtx.Constraints.Max.X
		children := make([]layout.FlexChild, 0, columns)
		for column := 0; column < columns; column++ {
			index := row*columns + column
			if index >= len(values) {
				children = append(children, layout.Flexed(1, func(layout.Context) layout.Dimensions { return layout.Dimensions{} }))
				continue
			}
			value := values[index]
			children = append(children, layout.Flexed(1, func(gtx layout.Context) layout.Dimensions {
				return w.layoutEmojiChoice(gtx, value)
			}))
		}
		return layout.Flex{Axis: layout.Horizontal, Alignment: layout.Middle}.Layout(gtx, children...)
	})
}

func emojiGridColumns(width, targetCellWidth int) int {
	if targetCellWidth <= 0 {
		return 4
	}
	return max(4, width/targetCellWidth)
}

// layoutEmojiGlyph draws one emoji and reports the box its INK occupies rather
// than the box its line occupies, so a centring parent centres the glyph.
//
// An emoji's ink fills the whole ascent and stops on the baseline: at 22sp the
// line box is 29px tall with 22px of glyph and 7px of empty descent under it.
// Centring that box splits the emptiness in two and lifts the glyph 3px above
// the middle of its cell — measurably off the hover highlight drawn around it.
//
// The glyph still DRAWS its descent below the reported box. That stays inside
// the cell while the cell can spare half a descent under the centred ink,
// which a 38dp cell around a 22sp emoji can.
func layoutEmojiGlyph(gtx layout.Context, label material.LabelStyle) layout.Dimensions {
	dims := label.Layout(gtx)
	return layout.Dimensions{Size: image.Pt(dims.Size.X, dims.Size.Y-dims.Baseline)}
}

func (w *Window) layoutEmojiChoice(gtx layout.Context, value string) layout.Dimensions {
	button := w.emojiPicker.emojiButtons[value]
	return button.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
		semantic.Button.Add(gtx.Ops)
		semantic.DescriptionOp(w.t("emoji.insert", value)).Add(gtx.Ops)
		side := gtx.Dp(emojiGridCellHeightDp)
		gtx.Constraints.Min.X = max(gtx.Constraints.Min.X, min(side, gtx.Constraints.Max.X))
		gtx.Constraints.Min.Y = side
		gtx.Constraints.Max.Y = side
		if button.Hovered() || gtx.Focused(button) {
			fillRounded(gtx, color.NRGBA{R: 36, G: 54, B: 76, A: 255}, unit.Dp(7))
		}
		return layout.Center.Layout(gtx, func(gtx layout.Context) layout.Dimensions {
			label := material.Label(w.theme, emojiGlyphSizeSp, value)
			label.Font.Typeface = font.Typeface("emoji")
			label.Color = color.NRGBA{R: 247, G: 249, B: 252, A: 255}
			return layoutEmojiGlyph(gtx, label)
		})
	})
}
