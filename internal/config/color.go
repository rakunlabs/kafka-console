package config

import "strings"

// normalizeColor returns c as lowercase "#rrggbb" or a CSS color name. It
// accepts #rgb and #rrggbb, with or without the "#", and the CSS named
// colors such as "blue" or "tomato".
func normalizeColor(c string) (string, bool) {
	c = strings.ToLower(strings.TrimSpace(c))
	if _, ok := cssColors[c]; ok {
		return c, true
	}

	c = strings.TrimPrefix(c, "#")
	if len(c) != 3 && len(c) != 6 {
		return "", false
	}

	for _, r := range c {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "", false
		}
	}

	if len(c) == 3 {
		c = string([]byte{c[0], c[0], c[1], c[1], c[2], c[2]})
	}

	return "#" + c, true
}

// cssColors are the CSS named colors browsers understand.
var cssColors = func() map[string]struct{} {
	m := map[string]struct{}{}
	for _, name := range strings.Fields(`
		aliceblue antiquewhite aqua aquamarine azure beige bisque black
		blanchedalmond blue blueviolet brown burlywood cadetblue chartreuse
		chocolate coral cornflowerblue cornsilk crimson cyan darkblue darkcyan
		darkgoldenrod darkgray darkgreen darkgrey darkkhaki darkmagenta
		darkolivegreen darkorange darkorchid darkred darksalmon darkseagreen
		darkslateblue darkslategray darkslategrey darkturquoise darkviolet
		deeppink deepskyblue dimgray dimgrey dodgerblue firebrick floralwhite
		forestgreen fuchsia gainsboro ghostwhite gold goldenrod gray green
		greenyellow grey honeydew hotpink indianred indigo ivory khaki lavender
		lavenderblush lawngreen lemonchiffon lightblue lightcoral lightcyan
		lightgoldenrodyellow lightgray lightgreen lightgrey lightpink
		lightsalmon lightseagreen lightskyblue lightslategray lightslategrey
		lightsteelblue lightyellow lime limegreen linen magenta maroon
		mediumaquamarine mediumblue mediumorchid mediumpurple mediumseagreen
		mediumslateblue mediumspringgreen mediumturquoise mediumvioletred
		midnightblue mintcream mistyrose moccasin navajowhite navy oldlace
		olive olivedrab orange orangered orchid palegoldenrod palegreen
		paleturquoise palevioletred papayawhip peachpuff peru pink plum
		powderblue purple rebeccapurple red rosybrown royalblue saddlebrown
		salmon sandybrown seagreen seashell sienna silver skyblue slateblue
		slategray slategrey snow springgreen steelblue tan teal thistle tomato
		turquoise violet wheat white whitesmoke yellow yellowgreen`) {
		m[name] = struct{}{}
	}

	return m
}()
