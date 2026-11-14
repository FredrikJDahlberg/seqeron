"""Builds the seqeron architecture and benchmarks deck."""
import sys
import uuid
from lxml import etree
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.enum.dml import MSO_THEME_COLOR as T, MSO_LINE
from pptx.enum.shapes import MSO_SHAPE, MSO_CONNECTOR
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.oxml.ns import qn

OUT = sys.argv[1]
import os
OFF = set(os.environ.get("OFF", "").split(","))

TX, BG, DK2, LT2 = T.TEXT_1, T.BACKGROUND_1, T.TEXT_2, T.BACKGROUND_2
A1, A2, A3, A4, A5, A6 = T.ACCENT_1, T.ACCENT_2, T.ACCENT_3, T.ACCENT_4, T.ACCENT_5, T.ACCENT_6
# A1 teal (sequenced, live)  A2 amber (synthesized, history)  A3 teal tint  A4 amber tint  A5 muted  A6 rule
COLORS = {"dk1": "15202B", "lt1": "FFFFFF", "dk2": "0F2A37", "lt2": "EEF2F5",
          "accent1": "0D5C78", "accent2": "B0601C", "accent3": "D9EAF0", "accent4": "F6E5D4",
          "accent5": "56636F", "accent6": "C9D2DA", "hlink": "0D5C78", "folHlink": "56636F"}
HEAD, BODY = "Georgia", "Arial"
W, H = 13.333, 7.5
L = 0.6
CW = W - 2 * L

prs = Presentation()
prs.slide_width, prs.slide_height = Inches(W), Inches(H)
NS = {"a": "http://schemas.openxmlformats.org/drawingml/2006/main"}


def I(v):
    return Emu(int(round(v * 914400)))


# ---------------------------------------------------------------- theme and layouts
def set_theme():
    part = prs.slide_master.part.part_related_by(RT.THEME)
    root = etree.fromstring(part.blob)
    cs = root.find(".//a:clrScheme", NS)
    cs.set("name", "seqeron")
    for tag, val in COLORS.items():
        el = cs.find("a:" + tag, NS)
        for c in list(el):
            el.remove(c)
        etree.SubElement(el, qn("a:srgbClr")).set("val", val)
    fs = root.find(".//a:fontScheme", NS)
    fs.set("name", "seqeron")
    fs.find("a:majorFont/a:latin", NS).set("typeface", HEAD)
    fs.find("a:minorFont/a:latin", NS).set("typeface", BODY)
    root.find(".//a:themeElements", NS).getparent().set("name", "seqeron")
    part._blob = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone=True)


if "theme" not in OFF:
    set_theme()
L_TITLE, L_SECTION, L_CONTENT = prs.slide_layouts[0], prs.slide_layouts[2], prs.slide_layouts[5]


def place(ph, x, y, w, h):
    ph.left, ph.top, ph.width, ph.height = I(x), I(y), I(w), I(h)


for lay in (L_TITLE, L_SECTION):
    lay.background.fill.solid()
    lay.background.fill.fore_color.theme_color = DK2
for ph in L_TITLE.placeholders:
    if ph.placeholder_format.idx == 0:
        place(ph, 0.8, 2.0, 11.7, 1.4)
    elif ph.placeholder_format.idx == 1:
        place(ph, 0.8, 3.45, 11.7, 1.0)
for ph in L_SECTION.placeholders:
    if ph.placeholder_format.idx == 0:
        place(ph, 0.8, 3.0, 11.7, 1.1)
    elif ph.placeholder_format.idx == 1:
        place(ph, 0.8, 4.15, 11.7, 0.8)
for ph in L_CONTENT.placeholders:
    if ph.placeholder_format.idx == 0:
        place(ph, L, 0.35, CW, 0.75)

A = "http://schemas.openxmlformats.org/drawingml/2006/main"
P = "http://schemas.openxmlformats.org/presentationml/2006/main"


def footer_sp(sid, name, x, w, para):
    return etree.fromstring(
        f'<p:sp xmlns:p="{P}" xmlns:a="{A}"><p:nvSpPr><p:cNvPr id="{sid}" name="{name}"/>'
        f'<p:cNvSpPr txBox="1"/><p:nvPr userDrawn="1"/></p:nvSpPr><p:spPr><a:xfrm>'
        f'<a:off x="{I(x)}" y="{I(7.0)}"/><a:ext cx="{I(w)}" cy="{I(0.3)}"/></a:xfrm>'
        f'<a:prstGeom prst="rect"><a:avLst/></a:prstGeom><a:noFill/></p:spPr><p:txBody>'
        f'<a:bodyPr wrap="none" lIns="0" tIns="0" rIns="0" bIns="0" anchor="ctr"/><a:lstStyle/>'
        f"{para}</p:txBody></p:sp>")


RPR = '<a:rPr lang="en-US" sz="1000" dirty="0"><a:solidFill><a:schemeClr val="accent5"/></a:solidFill></a:rPr>'
tree = L_CONTENT.shapes._spTree
if "footer" not in OFF: tree.append(footer_sp(901, "Footer", L, 6.0, f"<a:p xmlns:a=\"{A}\"><a:r>{RPR}<a:t>seqeron</a:t></a:r></a:p>"))


# ---------------------------------------------------------------- primitives
_n = [0]


def nm(kind):
    _n[0] += 1
    return f"{kind} {_n[0]}"


def runfmt(r, size=None, bold=None, color=None, italic=None, head=False):
    f = r.font
    if size:
        f.size = Pt(size)
    if bold is not None:
        f.bold = bold
    if italic is not None:
        f.italic = italic
    if color is not None:
        f.color.theme_color = color
    if head:
        f.name = "+mj-lt"
        r._r.get_or_add_rPr().set("cap", "none")


def fill_tf(tf, paras, size=14, color=TX, align=PP_ALIGN.LEFT, bold=False, space=0, head=False):
    """paras: str | list of str | list of dict(text|runs, size, bold, color, bullet, align, space, head)."""
    if isinstance(paras, str):
        paras = [paras]
    first = True
    for p in paras:
        if isinstance(p, str):
            p = {"text": p}
        para = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        para.alignment = p.get("align", align)
        sp = p.get("space", space)
        if sp:
            para.space_after = Pt(sp)
        runs = p.get("runs") or [(p.get("text", ""), {})]
        for txt, o in runs:
            r = para.add_run()
            r.text = txt
            runfmt(r, o.get("size", p.get("size", size)), o.get("bold", p.get("bold", bold)),
                   o.get("color", p.get("color", color)), o.get("italic", p.get("italic")),
                   o.get("head", p.get("head", head)))
        if p.get("bullet") and "bullets" not in OFF:
            pPr = para._p.get_or_add_pPr()
            pPr.set("marL", str(I(0.25)))
            pPr.set("indent", str(-I(0.25)))
            bc = etree.SubElement(pPr, qn("a:buClr"))
            etree.SubElement(bc, qn("a:schemeClr")).set("val", "accent1")
            etree.SubElement(pPr, qn("a:buChar")).set("char", "•")


def text(s, x, y, w, h, paras, size=14, color=TX, align=PP_ALIGN.LEFT, anchor=MSO_ANCHOR.TOP,
         bold=False, space=0, head=False, margin=0.0):
    tb = s.shapes.add_textbox(I(x), I(y), I(w), I(h))
    tb.name = nm("Text")
    tf = tb.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    for side in ("left", "right", "top", "bottom"):
        setattr(tf, "margin_" + side, I(margin))
    fill_tf(tf, paras, size, color, align, bold, space, head)
    return tb


STYLES = {
    # fill, line color, line width pt, dash
    "box": (BG, TX, 1.0, None),
    "host": (LT2, A6, 1.25, None),
    "mem": (LT2, A5, 1.0, MSO_LINE.DASH),
    "queue": (A3, A1, 1.25, None),
    "syn": (A4, A2, 1.25, None),
    "held": (A3, A1, 1.25, MSO_LINE.DASH),
    "miss": (None, A2, 1.25, MSO_LINE.DASH),
    "lead": (BG, A1, 2.5, None),
    "bar": (A1, None, 0, None),
    "dim": (A6, None, 0, None),
    "card": (LT2, None, 0, None),
    "dark": (DK2, None, 0, None),
    "accent": (A1, None, 0, None),
    "warm": (A2, None, 0, None),
    "plain": (BG, A6, 1.0, None),
    "frame": (None, A5, 1.0, MSO_LINE.DASH),
    "label": (BG, None, 0, None),
}


def shape(s, x, y, w, h, style="box", kind=MSO_SHAPE.RECTANGLE, name="Shape"):
    fill, line, width, dash = STYLES[style]
    sh = s.shapes.add_shape(kind, I(x), I(y), I(w), I(h))
    sh.name = nm(name)
    sh.shadow.inherit = False
    if fill is None:
        sh.fill.background()
    else:
        sh.fill.solid()
        sh.fill.fore_color.theme_color = fill
    if line is None:
        sh.line.fill.background()
    else:
        sh.line.color.theme_color = line
        sh.line.width = Pt(width)
        if dash:
            sh.line.dash_style = dash
    if kind == MSO_SHAPE.ROUNDED_RECTANGLE:
        sh.adjustments[0] = 0.08
    return sh


def box(s, x, y, w, h, title=None, sub=None, style="box", tsize=12, ssize=10, kind=MSO_SHAPE.RECTANGLE,
        align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE, tcolor=TX, scolor=A5, margin=0.05, name="Box"):
    sh = shape(s, x, y, w, h, style, kind, name)
    tf = sh.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    for side in ("left", "right", "top", "bottom"):
        setattr(tf, "margin_" + side, I(margin))
    paras = []
    if title:
        paras.append({"text": title, "size": tsize, "bold": True, "color": tcolor})
    if sub:
        paras.append({"text": sub, "size": ssize, "color": scolor})
    if paras:
        fill_tf(tf, paras, align=align)
    return sh


LINES = {"ln": (TX, 1.25, None), "a": (A1, 1.75, None), "w": (A2, 1.75, None), "d": (A5, 1.25, MSO_LINE.DASH),
         "lt": (A3, 1.25, None)}


def _style_line(ln_shape, style, head, tail_start=False):
    color, width, dash = LINES[style]
    ln_shape.line.color.theme_color = color
    ln_shape.line.width = Pt(width)
    if dash:
        ln_shape.line.dash_style = dash
    ln = ln_shape.line._get_or_add_ln()
    if "heads" in OFF: return
    if tail_start:
        etree.SubElement(ln, qn("a:headEnd")).attrib.update({"type": "triangle", "w": "med", "len": "med"})
    if head:
        etree.SubElement(ln, qn("a:tailEnd")).attrib.update({"type": "triangle", "w": "med", "len": "med"})


def arrow(s, pts, style="ln", head=True, both=False):
    if len(pts) == 2:
        (x1, y1), (x2, y2) = pts
        c = s.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, I(x1), I(y1), I(x2), I(y2))
        c.name = nm("Arrow")
        _style_line(c, style, head, both)
        return c
    if "freeform" in OFF: return None
    fb = s.shapes.build_freeform(I(pts[0][0]), I(pts[0][1]), scale=1.0)
    fb.add_line_segments([(I(x), I(y)) for x, y in pts[1:]], close=False)
    sh = fb.convert_to_shape()
    sh.name = nm("Path")
    sh.fill.background()
    sh.shadow.inherit = False
    _style_line(sh, style, head, both)
    return sh


def table(s, x, y, colw, rows, size=14, rowh=0.45, head_fill=A1):
    n, m = len(rows), len(colw)
    gt = s.shapes.add_table(n, m, I(x), I(y), I(sum(colw)), I(rowh * n))
    gt.name = nm("Table")
    tbl = gt.table
    tbl.horz_banding = False
    tbl.first_row = True
    for j, w in enumerate(colw):
        tbl.columns[j].width = I(w)
    for i, row in enumerate(rows):
        tbl.rows[i].height = I(rowh)
        for j, val in enumerate(row):
            c = tbl.cell(i, j)
            c.margin_left = c.margin_right = I(0.1)
            c.margin_top = c.margin_bottom = I(0.05)
            c.vertical_anchor = MSO_ANCHOR.MIDDLE
            c.fill.solid()
            if i == 0:
                c.fill.fore_color.theme_color = head_fill
                fill_tf(c.text_frame, [{"text": val, "bold": True, "color": BG, "size": size - 1}])
            else:
                c.fill.fore_color.theme_color = LT2 if i % 2 == 0 else BG
                fill_tf(c.text_frame, [{"text": val, "size": size, "bold": j == 0}])
    return gt


class Fig:
    """Maps a diagram drawn in abstract units onto the slide."""

    def __init__(self, s, ox, oy, sc):
        self.s, self.ox, self.oy, self.sc = s, ox, oy, sc

    def X(self, u):
        return self.ox + u * self.sc

    def Y(self, u):
        return self.oy + u * self.sc

    def box(self, x, y, w, h, title=None, sub=None, style="box", **kw):
        return box(self.s, self.X(x), self.Y(y), w * self.sc, h * self.sc, title, sub, style, **kw)

    def host(self, x, y, w, h, label, align=PP_ALIGN.CENTER, size=11):
        sh = shape(self.s, self.X(x), self.Y(y), w * self.sc, h * self.sc, "host", name="Host")
        tf = sh.text_frame
        tf.vertical_anchor = MSO_ANCHOR.TOP
        tf.margin_top, tf.margin_left, tf.margin_right = I(0.08), I(0.12), I(0.12)
        fill_tf(tf, [{"text": label.upper(), "size": size, "bold": True, "color": A5, "align": align}])
        return sh

    def label(self, x, y, w, h, t, size=10, color=A5, align=PP_ALIGN.CENTER, bold=False):
        return text(self.s, self.X(x), self.Y(y), w * self.sc, h * self.sc, t, size=size, color=color,
                    align=align, anchor=MSO_ANCHOR.MIDDLE, bold=bold)

    def arrow(self, pts, style="ln", head=True, both=False):
        return arrow(self.s, [(self.X(x), self.Y(y)) for x, y in pts], style, head, both)


def fits(label, size, w):
    """Warns of a one-line label likely to wrap: Arial averages about 0.5 em a character."""
    if len(label) * size * 0.5 / 72 > w:
        print(f"wraps at {w:.2f} in: {label}")


class Seq:
    """A sequence diagram: lanes left to right, rows top to bottom from a cursor."""

    def __init__(self, s, lanes, xs, y=1.58, bottom=6.85, row=0.32,
                 legend=(("live queue", "a"), ("replay 201", "w"), ("request", "ln"), ("reply", "d"))):
        self.s, self.xs, self.bottom, self.row = s, xs, bottom, row
        self.frames = []
        for (name, sub, style), x in zip(lanes, xs):
            box(s, x - 1.05, y, 2.1, 0.46, name, sub, style=style, tsize=12, ssize=9, name="Lane")
        for x in xs:
            arrow(s, [(x, y + 0.46), (x, bottom)], "lt", head=False)
        self.y = y + 0.46
        self.legend(legend)

    def legend(self, items):
        x = W - L - 1.1 * len(items)
        for lab, st in items:
            arrow(self.s, [(x, 1.31), (x + 0.35, 1.31)], st)
            text(self.s, x + 0.42, 1.19, 0.68, 0.24, lab, size=10, color=A5, anchor=MSO_ANCHOR.MIDDLE)
            x += 1.1

    def msg(self, a, b, label, style="ln", lost=False):
        """A message from lane a to lane b, labelled above its line."""
        self.y += self.row
        x1, x2 = self.xs[a], self.xs[b]
        lo, hi = min(x1, x2), max(x1, x2)
        fits(label, 11, hi - lo - 0.1)
        text(self.s, lo + 0.05, self.y - 0.27, hi - lo - 0.1, 0.25, label, size=11,
             color=A2 if lost else TX, align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.BOTTOM)
        if lost:
            xe = x1 + (x2 - x1) * 0.62
            arrow(self.s, [(x1, self.y), (xe, self.y)], "w", head=False)
            text(self.s, xe - 0.1, self.y - 0.14, 0.2, 0.28, "×", size=16, bold=True, color=A2,
                 align=PP_ALIGN.CENTER, anchor=MSO_ANCHOR.MIDDLE)
        else:
            arrow(self.s, [(x1, self.y), (x2, self.y)], style)

    def note(self, lane, label, w=4.1, style="card"):
        """A step a lane takes on its own, to the right of its lifeline."""
        self.y += self.row
        fits(label, 11, w - 0.16)
        box(self.s, self.xs[lane] + 0.08, self.y - 0.27, w, 0.3, None, None, style=style, name="Note")
        text(self.s, self.xs[lane] + 0.16, self.y - 0.27, w - 0.16, 0.3, label, size=11, anchor=MSO_ANCHOR.MIDDLE)

    def _tag(self, tag, cond, x, y):
        box(self.s, x, y, 0.62, 0.24, tag, None, style="dim", tsize=9, margin=0, name="Frame tag")
        if cond:
            shape(self.s, x + 0.66, y, 0.12 + 0.072 * len(cond), 0.24, "label", name="Frame guard")
            text(self.s, x + 0.72, y, 0.072 * len(cond), 0.24, [{"text": cond, "italic": True}], size=10,
                 color=A5, anchor=MSO_ANCHOR.MIDDLE).text_frame.word_wrap = False

    def begin(self, tag, cond, x=L, w=CW):
        """Opens an alt or loop frame; the guard sits in its top row."""
        self.y += 0.12
        self.frames.append((x, w, self.y))
        self._tag(tag, cond, x, self.y)
        self.y += 0.15

    def orelse(self, cond):
        x, w, _ = self.frames[-1]
        self.y += 0.14
        arrow(self.s, [(x, self.y), (x + w, self.y)], "d", head=False)
        self._tag("ELSE", cond, x, self.y)
        self.y += 0.15

    def end(self):
        x, w, top = self.frames.pop()
        self.y += 0.12
        shape(self.s, x, top, w, self.y - top, "frame", name="Frame")


# ---------------------------------------------------------------- slide kinds
SECTIONS = []  # (name, [slide ids])


def section(name):
    SECTIONS.append((name, []))


def _track(s):
    SECTIONS[-1][1].append(s.slide_id)


def notes(s, t):
    s.notes_slide.notes_text_frame.text = t


def content(title, kicker=None, note=""):
    s = prs.slides.add_slide(L_CONTENT)
    _track(s)
    t = s.shapes.title
    tf = t.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = MSO_ANCHOR.TOP
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    fill_tf(tf, [{"text": title, "size": 34, "bold": True, "color": TX, "head": True}])
    if kicker:
        text(s, L, 1.08, CW, 0.45, kicker, size=16, color=A5)
    s.shapes._spTree.append(footer_sp(9000 + len(prs.slides), "Slide number", W - L - 1.0, 1.0,
                            f'<a:p xmlns:a="{A}"><a:pPr algn="r"/><a:fld id="{{{uuid.uuid4()}}}" type="slidenum">'
                            f'{RPR}<a:t>{len(prs.slides)}</a:t></a:fld></a:p>'))
    notes(s, note)
    return s


def tape(s, x, y, n, current, cw=0.42, gap=0.08, on_dark=True):
    """The deck's motif: a run of clusterSeqNo cells, the current one filled."""
    for i in range(n):
        st = "accent" if i + 1 == current else ("dark" if on_dark else "card")
        sh = shape(s, x + i * (cw + gap), y, cw, cw, st, name="Tape cell")
        if on_dark and i + 1 != current:
            sh.line.color.theme_color = A1
            sh.line.width = Pt(1.25)
        tf = sh.text_frame
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        fill_tf(tf, [{"text": str(i + 1), "size": 12, "bold": i + 1 == current,
                      "color": BG if i + 1 == current else A3, "align": PP_ALIGN.CENTER}])


def log_cells(s, x, y, w=0.31, h=0.36, step=0.346):
    """A queue as a log: frames 1-4 written, 5 being written, two free cells."""
    for i in range(7):
        st = "accent" if i == 4 else ("queue" if i < 4 else "plain")
        sh = shape(s, x + i * step, y, w, h, st, name="Queue cell")
        tf = sh.text_frame
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        if i < 5:
            fill_tf(tf, [{"text": str(i + 1), "size": 9, "bold": i == 4, "color": BG if i == 4 else A1,
                          "align": PP_ALIGN.CENTER}])


def num_circle(s, x, y, n, d=0.5, style="accent", size=16):
    sh = shape(s, x, y, d, d, style, MSO_SHAPE.OVAL, "Number")
    tf = sh.text_frame
    tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = MSO_ANCHOR.MIDDLE
    fill_tf(tf, [{"text": str(n), "size": size, "bold": True, "color": BG, "align": PP_ALIGN.CENTER}])
    return sh


def card(s, x, y, w, h, paras, style="card", margin=0.2, anchor=MSO_ANCHOR.TOP):
    sh = shape(s, x, y, w, h, style, name="Card")
    tf = sh.text_frame
    tf.word_wrap = True
    tf.vertical_anchor = anchor
    for side in ("left", "right", "top", "bottom"):
        setattr(tf, "margin_" + side, I(margin))
    fill_tf(tf, paras)
    return sh


C, M, R = PP_ALIGN.CENTER, MSO_ANCHOR.MIDDLE, PP_ALIGN.RIGHT


from pptx.chart.data import CategoryChartData
from pptx.enum.chart import XL_CHART_TYPE, XL_LABEL_POSITION, XL_LEGEND_POSITION, XL_TICK_LABEL_POSITION, XL_TICK_MARK
from pptx.dml.color import RGBColor


def rgb(name):
    return RGBColor.from_string(COLORS[name])


def chart(s, x, y, w, h, cats, series, colors, horizontal=False, fmt="#,##0", size=12, legend=False, top=None,
          point_colors=None):
    """A clustered bar chart in the deck's palette; colors names a series each, point_colors a point each."""
    cd = CategoryChartData()
    cd.categories = cats
    for name, vals in series:
        cd.add_series(name, vals)
    kind = XL_CHART_TYPE.BAR_CLUSTERED if horizontal else XL_CHART_TYPE.COLUMN_CLUSTERED
    gf = s.shapes.add_chart(kind, I(x), I(y), I(w), I(h), cd)
    gf.name = nm("Chart")
    ch = gf.chart
    ch.font.size = Pt(size)
    ch.font.name = BODY
    ch.font.color.rgb = rgb("dk1")
    ch.has_title = False
    ch.has_legend = legend
    if legend:
        ch.legend.position = XL_LEGEND_POSITION.BOTTOM
        ch.legend.include_in_layout = False
        ch.legend.font.size = Pt(size)
    va = ch.value_axis
    va.has_major_gridlines = False
    va.tick_label_position = XL_TICK_LABEL_POSITION.NONE
    va.major_tick_mark = XL_TICK_MARK.NONE
    va.format.line.fill.background()
    va.minimum_scale = 0
    if top:
        va.maximum_scale = top
    ca = ch.category_axis
    ca.format.line.color.rgb = rgb("accent6")
    ca.tick_labels.font.size = Pt(size)
    if horizontal:
        ca.reverse_order = True
    plot = ch.plots[0]
    plot.gap_width = 50
    plot.has_data_labels = True
    dl = plot.data_labels
    dl.font.size = Pt(size)
    dl.font.bold = True
    dl.number_format = fmt
    dl.number_format_is_linked = False
    dl.position = XL_LABEL_POSITION.OUTSIDE_END
    for i, ser in enumerate(plot.series):
        ser.format.fill.solid()
        ser.format.fill.fore_color.rgb = rgb(colors[i])
        if point_colors:
            for j, c in enumerate(point_colors):
                pt = ser.points[j]
                pt.format.fill.solid()
                pt.format.fill.fore_color.rgb = rgb(c)
    return gf


def stat(s, x, y, w, h, big, lab, d, bsize=28):
    card(s, x, y, w, h, [{"text": big, "size": bsize, "bold": True, "color": A1, "head": True, "space": 2},
                         {"text": lab, "size": 14, "bold": True, "space": 3},
                         {"text": d, "size": 13, "color": A5}], margin=0.2)


# ================================================================= OPENING
section("Opening")
s = prs.slides.add_slide(L_TITLE)
_track(s)
for ph in s.placeholders:
    tf = ph.text_frame
    tf.margin_left = tf.margin_right = 0
    if ph.placeholder_format.idx == 0:
        fill_tf(tf, [{"text": "seqeron", "size": 66, "bold": True, "color": BG, "head": True}])
    else:
        fill_tf(tf, [{"text": "Architecture and benchmarks", "size": 26, "color": A3}])
tape(s, 0.8, 5.2, 12, 7, cw=0.52, gap=0.08)
text(s, 0.8, 5.85, 11.7, 0.4, "A replicated sequencer on Aeron Cluster  ·  seqeron 0.18.0  ·  Aeron 1.53.2", size=14, color=A3)
url = text(s, 0.8, 6.25, 11.7, 0.4, "github.com/FredrikJDahlberg/seqeron", size=14, color=A3)
url.click_action.hyperlink.address = "https://github.com/FredrikJDahlberg/seqeron"
notes(s, "About 30 minutes: 12 on the architecture, 10 on the benchmarks, the rest for discussion. seqeron is the "
         "sequencing tier: an Aeron Cluster replicated state machine that gives messages from any number of producers "
         "one global, gap-free total order, plus the replay service and client library that deliver that order to every "
         "consumer. The row of cells is the deck's motif: the log, numbered by clusterSeqNo.")

s = content("Summary", "The architecture in one slide, what was measured, and what was not",
            "The one-minute version. seqeron gives every producer's messages one global, gap-free order and every consumer "
            "the same complete history on its own host. Since 0.18.0 a member records only the Raft log; every queue is "
            "derived from it. The benchmarks ran on three c6in.8xlarge members with 100-byte messages, which is the "
            "production size. The figures are one launch: say so before anyone asks, and say which results are unexplained.")
summary = [("Architecture", A1, ["A Raft-replicated sequencer on Aeron Cluster: one order for every producer",
                                  "The Raft log is the one copy of history; each queue is derived from it",
                                  "Complete, gap-free history on every consumer's own host",
                                  "Cluster in Java; clients in Java, C++ and C#"]),
           ("Measured on EC2", A2, ["909k msg/s of 100-byte messages held; saturated at 1.14M",
                                   "p99.9 of 0.13–0.23 ms in most steps; 0.8–22 ms in four",
                                   "No regular young-GC pause: a leader takes three in a 210 s ramp",
                                   "Cold replay of 4,000,000 frames in 5.3 s"]),
           ("Not yet shown", TX, ["A second launch of the same instances",
                                  "Many producers; a leadership change timed to the first sequenced command",
                                  "Derived members' latency below 50k msg/s",
                                  "Two results the sessions leave unexplained"])]
text(s, L, 1.75, CW, 0.8, "Three members, one total order, a log every member applies identically. Open source, Apache 2.0.",
     size=17)
for i, (t, c, pts) in enumerate(summary):
    x = L + i * (3.84 + 0.305)
    card(s, x, 2.5, 3.84, 3.9, [{"text": t, "size": 18, "bold": True, "head": True, "color": c, "space": 8}]
         + [{"text": p, "size": 14, "bullet": True, "space": 8} for p in pts], margin=0.22)
text(s, L, 6.5, CW, 0.4, [{"runs": [("Every figure is one launch. ", {"bold": True, "color": A2}),
                                    ("The benchmark slides say what each result is and is not.", {})]}], size=14)


s = content("One replicated order",
            "Every input is stamped once; every service is a deterministic state machine over the result",
            "The sequencer architecture: services never coordinate with each other. Two replicas that apply the same "
            "stream hold the same state; a hot standby is a replica that does not publish; an audit is a replay; a "
            "restart is a replay from a known point. The sequencer then becomes the component everything depends on, "
            "so seqeron makes it a Raft-replicated state machine that survives the loss of a minority of nodes and "
            "continues under a new leader with no gap and no renumbering. Payloads are opaque: the cluster copies "
            "them through unopened.")
for i, t in enumerate(["Gateway pairs", "Applications", "Operator (clusterctl)"]):
    box(s, L, 1.95 + i * 0.8, 2.5, 0.6, t, style="box", tsize=13)
    arrow(s, [(L + 2.5, 2.25 + i * 0.8), (3.65, 3.05)], "ln")
box(s, 3.7, 2.15, 2.6, 1.8, "Sequencer cluster", "Raft · 3 to 7 cluster members", style="lead", tsize=15, ssize=12)
arrow(s, [(6.3, 3.05), (6.75, 3.05)], "a")
for i in range(6):
    box(s, 6.8 + i * 0.6, 2.78, 0.54, 0.54, str(101 + i), style="queue", tsize=12)
text(s, 6.8, 2.25, 3.6, 0.4, "clusterSeqNo: no gaps, no reuse", size=12, color=A5)
arrow(s, [(10.36, 3.05), (10.85, 3.05)], "a")
card(s, 10.9, 1.95, 1.83, 2.2, [{"text": "Every consumer", "bold": True, "size": 13},
                                 {"text": "applications", "size": 13, "bullet": True},
                                 {"text": "gateways", "size": 13, "bullet": True},
                                 {"text": "audit, risk", "size": 13, "bullet": True}], style="plain", margin=0.12)
stats = [("1", "stream", "one total order across every producer"),
         ("3–7", "members", "a minority can fail with no gap and no renumbering"),
         ("16", "groups", "an ordered queue per group, one clusterSeqNo across all"),
         ("1 Hz", "cluster clock", "a heartbeat on consensus time every client agrees on")]
for i, (big, lab, d) in enumerate(stats):
    x = L + i * (2.85 + 0.243)
    card(s, x, 4.75, 2.85, 1.55, [{"runs": [(big, {"size": 36, "bold": True, "color": A1, "head": True}),
                                            ("  " + lab, {"size": 14, "bold": True, "color": TX})]},
                                  {"text": d, "size": 13, "color": A5}], margin=0.18)


# ================================================================= ARCHITECTURE
section("Architecture")

s = content("A member and a client", None,
            "Each member is one JVM: consensus module, sequencer, embedded media driver, archive and Replayer. The "
            "archive records the Raft log and nothing else. Live data reaches a client straight off the queue, "
            "untethered, one queue per group. History comes through the Replayer, which derives it from the log's "
            "recording. Ingress is IPC when this member leads and UDP otherwise. Other members run the same pipeline and "
            "produce the same queues, frame for frame. The client tier is Java, C++ and C#, with C# deployed on Windows "
            "too; the member is Java only.")
sc = 5.25 / 440
f = Fig(s, (W - 960 * sc) / 2, 1.5, sc)
f.host(10, 60, 170, 90, "Other members")
f.label(10, 95, 170, 40, "same pipeline, same frames", size=11)
f.arrow([(95, 60), (95, 22), (590, 22), (590, 80)], "d", both=True)
f.label(240, 0, 200, 20, "Raft log replication")
f.host(210, 40, 510, 380, "Cluster member · one JVM", align=PP_ALIGN.LEFT)
f.box(230, 80, 200, 56, "Sequencer", "number · validate · synthesize")
f.box(480, 80, 220, 56, "Consensus module", "Raft log · cluster time")
f.arrow([(480, 108), (430, 108)])
f.label(230, 138, 200, 22, "Queue per group · aeron:ipc", size=10, color=A1, align=PP_ALIGN.LEFT, bold=True)
log_cells(s, f.X(230), f.Y(162), 26 * sc, 30 * sc, 29 * sc)
f.arrow([(359, 136), (359, 162)])
f.label(230, 215, 200, 60, "a queue is never recorded: the Raft log is the one copy of history", size=11, align=PP_ALIGN.LEFT)
f.box(480, 190, 220, 56, "Archive", "records the Raft log")
f.arrow([(590, 136), (590, 190)])
f.box(480, 270, 220, 56, "Replayer", "derives the queues from the log", "syn")
f.arrow([(590, 246), (590, 270)])
f.box(230, 350, 470, 50, "Embedded media driver", "queue · replay · request · control streams", "mem")
f.host(760, 60, 190, 290, "Client process")
f.box(772, 100, 166, 56, "ClusterStreamSender", "+ PendingSends", tsize=11)
f.box(772, 190, 166, 56, "ReplayerStreamReceiver", "+ ReplayerRecovery", tsize=11)
f.label(760, 280, 190, 46, "inside the Gateway or Application façade", size=10)
f.label(760, 318, 190, 24, "Java · C++ · C#", size=10, bold=True)
f.arrow([(772, 120), (700, 120)])
f.label(700, 96, 72, 22, "ingress")
f.arrow([(433, 177), (745, 177), (745, 212), (772, 212)], "a")
f.label(600, 150, 130, 22, "live · untethered", color=A1)
f.arrow([(700, 298), (772, 240)], "w")
f.label(712, 302, 60, 22, "replay", color=A2)

s = content("The life of a message", "Six steps from a producer's offer to its own confirmation",
            "1. IngressPublisher wraps the producer's own encoding in an Unsequenced frame and offers it. 2. The leader "
            "appends it to the Raft log; it commits at a majority. 3. Every member's sequencer validates it, assigns the "
            "next clusterSeqNo and consensus timestamp, and publishes it on its own queue; the payload is copied, never "
            "decoded. 4. The archive has already recorded the log entry, which is what a replay derives the frame from. "
            "5. Each client reads it off its local queue, checks continuity, dispatches. 6. The producer sees its own "
            "frame on its queue; only then is the message known to be sequenced. A cold start runs steps 5 and 6 "
            "against a replay first.")
steps = [("Offer", "Producer wraps its payload in a 28-byte envelope."),
         ("Commit", "Leader appends to the Raft log; a majority holds it."),
         ("Sequence", "Every member numbers, stamps and publishes on its own queue."),
         ("Record", "The archive records the Raft log entry; replays derive from it."),
         ("Deliver", "Each client reads its local queue, checks continuity."),
         ("Confirm", "The producer sees its own frame. Now it is sequenced.")]
sw, sg = 1.86, 0.194
arrow(s, [(L + 0.5, 2.25), (L + 5 * (sw + sg) + 0.5, 2.25)], "lt", head=False)
for i, (t, d) in enumerate(steps):
    x = L + i * (sw + sg)
    num_circle(s, x + 0.25, 2.0, i + 1, style="warm" if i == 5 else "accent")
    text(s, x, 2.75, sw, 0.45, t, size=18, bold=True, head=True)
    text(s, x, 3.25, sw - 0.1, 1.9, d, size=14)
card(s, L, 5.35, CW, 1.25, [{"runs": [("A send that succeeds is not a message sequenced. ", {"bold": True}),
                                      ("The cluster confirms nothing on egress; step 6 is the producer's own confirmation, "
                                       "on its own queue.", {})], "size": 16}], margin=0.25, anchor=M)

s = content("Derived queues", "The Raft log is the one copy of history; a queue is a function of it",
            "Every member applies the same committed log with the same code, so every member's queue carries the same "
            "frames in the same order. The archive records the Raft log and nothing of the member's queues. The Replayer "
            "runs a sequencer of its own over that recording, which yields the frames the queue carried, and keeps a "
            "checkpoint every 8 MiB of log so a replay can start mid-history. It is off the live path; nothing reads the "
            "archive except through it. The cost is replay rate: a third to a half of what an archive replays a "
            "recording at, and the checkpoints are rebuilt at each start before it serves. A client checks that its "
            "first frame is groupSeqNo 1 and that a resume opens where it asked, and stops otherwise. The comparison arm "
            "records its queues as well as the log: twice the archive bytes. Figures: the 11 and 13 November EC2 sessions.")
text(s, 2.7, 1.62, 3.0, 0.3, "LIVE PATH", size=11, bold=True, color=A1)
text(s, 2.7, 3.62, 3.0, 0.3, "HISTORY, OFF THE LIVE PATH", size=11, bold=True, color=A2)
box(s, L, 2.0, 1.6, 3.0, "Raft log", "recorded by the archive", style="lead", tsize=15, ssize=11)
box(s, 2.7, 1.95, 1.7, 0.9, "Sequencer", "applies each commit", style="box", tsize=14)
box(s, 4.9, 1.95, 1.7, 0.9, "Queue", "one per group", style="queue", tsize=14)
box(s, 2.7, 3.95, 1.7, 0.9, "Replayer", "runs a sequencer over the log", style="syn", tsize=14)
box(s, 4.9, 3.95, 1.7, 0.9, "Replay", "same frames, any start", style="held", tsize=14)
arrow(s, [(2.2, 2.4), (2.7, 2.4)])
arrow(s, [(4.4, 2.4), (4.9, 2.4)], "a")
arrow(s, [(6.6, 2.4), (7.0, 2.4)], "a")
arrow(s, [(2.2, 4.4), (2.7, 4.4)])
arrow(s, [(4.4, 4.4), (4.9, 4.4)], "w")
arrow(s, [(6.6, 4.4), (7.0, 4.4)], "w")
text(s, 7.05, 2.15, 1.1, 0.5, "clients, live", size=12, color=A1)
text(s, 7.05, 4.15, 1.1, 0.5, "clients, history", size=12, color=A2)
card(s, L, 5.35, 7.55, 1.25, [{"runs": [("Same log, same code, same frames. ", {"bold": True}),
                                        ("The Replayer is the sequencer run again over the recording, so no queue is stored "
                                         "and none can disagree with the log.", {})], "size": 15}], margin=0.22, anchor=M)
stat(s, 8.5, 1.85, 4.23, 1.5, "1.15 vs 2.30 GB", "a member's archive, 4,000,000 frames",
     "derived, against an arm that also records its queues", 26)
stat(s, 8.5, 3.5, 4.23, 1.5, "3.7 vs 7.3 GB", "archive growth in 600 s at 49k msg/s",
     "the same two arms", 26)
stat(s, 8.5, 5.15, 4.23, 1.5, "⅓ to ½", "of an archive's replay rate",
     "replay is computed; a checkpoint every 8 MiB of log", 26)

s = content("The frame", "Copy 20 bytes from ingress, append 24, never touch the payload",
            "Schema 210, two families. The application family carries an opaque payload named by payloadId: Unsequenced "
            "on ingress, Sequenced on the queue. The system family is seqeron's own vocabulary, named by systemEventType "
            "at the same offset 16. Every field is at the same offset in every template, so a consumer tracks continuity "
            "without branching on message type. sessionId is overwritten by the sequencer; the other ingress fields are "
            "copied. Groups: the topology declares up to 16, each with its own queue. A producer names the groups in "
            "groupMask; a system message is on every queue. clusterSeqNo is shared by all queues, so it jumps along one, "
            "and groupSeqNo, the queue's own number, does not. Consumers split by family first, then dispatch on "
            "(payloadId, templateId), never templateId alone.")
sc = CW / 880
f = Fig(s, L, 1.75, sc)
for u, lab in [(0, "0"), (80, "4"), (160, "8"), (320, "16"), (360, "18"), (400, "20"), (560, "28"), (720, "36"),
               (880, "44")]:
    f.label(u - 20, 10, 40, 22, lab, size=11, color=A5)
f.box(0, 40, 80, 48, "sourceId", tsize=11)
f.box(80, 40, 80, 48, "connectionId", tsize=11)
f.box(160, 40, 160, 48, "sessionId", "set by the sequencer", "syn")
f.box(320, 40, 40, 48, "payloadId", tsize=7, margin=0)
f.box(360, 40, 40, 48, "groupMask", tsize=7, margin=0)
f.box(400, 40, 160, 48, "clusterSeqNo", style="queue", tsize=13)
f.box(560, 40, 160, 48, "timestamp", "Raft consensus time, ns", "queue")
f.box(720, 40, 160, 48, "groupSeqNo", "the queue's own number", "queue")
f.arrow([(0, 98), (0, 106), (398, 106), (398, 98)], "ln", head=False)
f.label(0, 110, 400, 26, "from ingress · 20 bytes, sessionId rewritten", size=12, color=TX)
f.arrow([(402, 98), (402, 106), (880, 106), (880, 98)], "a", head=False)
f.label(402, 110, 478, 26, "appended by the sequencer · 24 bytes", size=12, color=A1)
f.label(0, 140, 880, 24, "Offset 16 is payloadId in the application family and systemEventType in the system family.", size=12)
fam = [("Application family", "Unsequenced (100) → Sequenced (101)",
        "One opaque payload named by payloadId. The cluster decodes none of it."),
       ("System family", "Named by systemEventType",
        "Thirteen events a producer submits; five the sequencer synthesizes, such as LeadershipChanged and ClusterHeartbeat."),
       ("Groups", "Up to 16, one queue each",
        "A producer names the groups in groupMask. clusterSeqNo is shared and jumps along a queue; groupSeqNo does not.")]
for i, (t, sub, d) in enumerate(fam):
    x = L + i * (3.84 + 0.305)
    card(s, x, 4.15, 3.84, 2.4, [{"text": t, "bold": True, "size": 18, "head": True, "space": 6},
                                 {"text": sub, "size": 14, "bold": True, "color": A1, "space": 6},
                                 {"text": d, "size": 14}])

s = content("Repairing a gap", "Replay only what was lost; hold live frames meanwhile",
            "A gap is a live frame whose groupSeqNo is past the next expected one. The client clears isCaughtUp and "
            "requests a resume at the last frame it delivered. That frame's clusterSeqNo is the anchor: the resumed "
            "replay must open on it, and if not, the recording does not match the positions the client holds, and the "
            "client stops. De-duplication goes by clusterSeqNo, which increases along a queue but jumps. Live frames past "
            "the gap go into a bounded FIFO, 65,536 frames or 16 MiB, and drain when the replay reaches them, so recovery "
            "ends without a second gap; past either bound it drops them and resumes again. A replay with no progress for "
            "5 s is requested again. A cold start is the same walk from groupSeqNo 1. All of it is ReplayerRecovery, a "
            "pure state machine with case-for-case tests and seeded property tests in Java, C++ and C#.")
sc = CW / 760
f = Fig(s, L - 10 * sc, 1.75, sc)
for lab, y in (("Queue", 40), ("Replay", 110), ("Delivered", 180)):
    f.label(10, y, 140, 34, lab, size=14, color=TX, align=PP_ALIGN.LEFT, bold=True)
queuest = {105: "miss", 106: "held", 107: "held", 108: "held"}
for k in range(10):
    n, x = 101 + k, 160 + k * 58
    st = queuest.get(n, "queue")
    f.box(x, 40, 50, 34, "×" if n == 105 else str(n), style=st, tsize=13, tcolor=A2 if n == 105 else TX)
    f.box(x, 180, 50, 34, str(n), style="syn" if n == 105 else "queue", tsize=13)
f.box(334, 110, 50, 34, "104", style="plain", tsize=13)
f.box(392, 110, 50, 34, "105", style="syn", tsize=13)
f.arrow([(417, 76), (417, 108)], "d")
f.label(450, 6, 300, 30, "106 arrives, 105 expected: resume at 104", size=12, color=A2, align=PP_ALIGN.LEFT)
f.label(450, 80, 300, 26, "106–108 retained in a bounded FIFO", size=12, color=A1, align=PP_ALIGN.LEFT)
f.label(450, 112, 300, 30, "anchor: reopens on 104's clusterSeqNo", size=12, align=PP_ALIGN.LEFT)
f.label(160, 222, 590, 28, "104 is a duplicate and dropped; 105 from the replay, then the FIFO drains, then back on the queue.",
        size=12, align=PP_ALIGN.LEFT)
text(s, L, 5.75, CW, 1.0, [{"runs": [("Bounds. ", {"bold": True}),
                                    ("The FIFO holds 65,536 frames or 16 MiB; a replay with no progress for 5 s is requested "
                                     "again.", {})], "space": 4},
                           {"runs": [("Cold start ", {"bold": True}),
                                     ("is the same walk from groupSeqNo 1 to the reported end, then the queue, "
                                      "de-duplicating on clusterSeqNo.", {})]}], size=14)


section("Producers and failures")

s = content("Two kinds of producer", "A façade per kind; the application writes its logic and none of seqeron's",
            "A gateway is an edge producer deployed as an active/hot-standby pair named in the topology; the cluster "
            "designates the active instance. An application runs one instance on every member, and a member can host "
            "several applications; LeadershipChanged already picks one instance of each, so only the one on the leader's "
            "host publishes. Or it runs remote, as the one instance on its own "
            "host: its gate opens once caught up, whoever leads, and it submits over UDP. Nothing elects "
            "between two remote instances, so a remote service that needs a standby is a gateway. Both façades run one single-threaded "
            "duty cycle on the caller's thread: doWork() returns a work count for the caller's idle strategy. The client "
            "tier starts no threads, and dispatch and confirmed publish allocate nothing once warm, which the C# suite "
            "tests; Java and C++ have no such test. The deployment is "
            "declared in one XML topology document that clusterctl load-topology puts on the log.")
for i, (t, sub, pts) in enumerate([
        ("Gateway", "active / hot-standby pair",
         ["Exchange session, client connection, feed handler", "The cluster designates the active instance",
          "Owns its external connections and a connection-id space"]),
        ("Application", "one instance on every member, or remote",
         ["Deterministic service over the stream; several per member", "On a member: publishes only while that member leads",
          "Remote: one instance on its own host, ingress over UDP",
          "Owns leader-only work: replies, outbound effects"])]):
    x = L + i * (5.9 + 0.33)
    shape(s, x, 1.85, 5.9, 3.0, "card", name="Producer")
    text(s, x + 0.25, 2.0, 5.4, 0.5, t, size=22, bold=True, head=True)
    text(s, x + 0.25, 2.5, 5.4, 0.35, sub, size=13, color=A1, bold=True)
    text(s, x + 0.25, 3.0, 5.4, 1.8, [{"text": p, "bullet": True, "space": 6} for p in pts], size=14)
text(s, L, 5.1, CW, 0.35, "EACH FAÇADE'S doWork(), ON THE CALLER'S THREAD", size=12, bold=True, color=A5)
chips = ["Cluster session", "Queue + recovery", "Confirmed ingress", "Election or leader gate", "Fences"]
cwid = (CW - 4 * 0.3) / 5
for i, c in enumerate(chips):
    x = L + i * (cwid + 0.3)
    box(s, x, 5.5, cwid, 0.7, c, style="queue", tsize=14, kind=MSO_SHAPE.ROUNDED_RECTANGLE)
    if i < 4:
        arrow(s, [(x + cwid, 5.85), (x + cwid + 0.3, 5.85)], "a")
text(s, L, 6.3, CW, 0.65, [
    {"runs": [("Fence: ", {"bold": True}), ("an instance that can no longer trust its view of the log closes its edge or "
                                           "leader gate, releases its session, reports once and stops acting.", {})], "space": 2},
    {"runs": [("Topology: ", {"bold": True}), ("one document declares both kinds; clusterctl load-topology puts it on "
                                              "the log.", {})]}], size=13, color=A5)


s = content("Confirmed ingress", "A failover can lose uncommitted ingress silently; the producer's own queue finds it",
            "A send returns once the frame is on the leader's ingress publication; the cluster confirms nothing. Frames "
            "the old leader had not committed are lost, and the session survives, so neither side sees a fault: about "
            "5,500 frames at 100 µs pacing in the failover harness. PendingSends holds each placed frame until the "
            "producer's own queue shows it, matched by cluster session. A LeadershipChanged with a newer term identifies "
            "exactly which were lost: the newest frames stamped with the old term that have not appeared. Nothing new is "
            "placed until they are resent, oldest first. Each frame is then sequenced exactly once, in order. The limit: "
            "this is in memory, so a restarted producer or a promoted standby starts with nothing pending, and a replaced "
            "session or a superseded gateway instance drops what was pending.")
text(s, L, 2.2, 1.8, 0.6, "Producer places", size=14, bold=True, anchor=M)
text(s, L, 3.6, 1.8, 0.6, "Own queue shows", size=14, bold=True, anchor=M)
cw_, cg = 0.75, 0.08
X0 = 2.5
for k in range(6):
    x = X0 + k * (cw_ + cg)
    lost = k >= 3
    box(s, x, 2.2, cw_, 0.6, f"f{k + 1}", style="miss" if lost else "queue", tsize=13, tcolor=A2 if lost else TX)
for k in range(3):
    box(s, X0 + k * (cw_ + cg), 3.6, cw_, 0.6, f"f{k + 1}", style="queue", tsize=13)
box(s, X0 + 3 * (cw_ + cg), 3.6, 2 * cw_ + cg + 0.6, 0.6, "LeadershipChanged", "term T+1", style="syn", tsize=12, ssize=10)
R0 = 8.0
for k in range(4):
    x = R0 + k * (cw_ + cg)
    lab = f"f{k + 4}"
    box(s, x, 2.2, cw_, 0.6, lab, style="syn" if k < 3 else "queue", tsize=13)
    box(s, x, 3.6, cw_, 0.6, lab, style="queue", tsize=13)
arrow(s, [(X0 + 3 * (cw_ + cg), 2.92), (X0 + 3 * (cw_ + cg), 3.02), (X0 + 6 * (cw_ + cg) - cg, 3.02),
          (X0 + 6 * (cw_ + cg) - cg, 2.92)], "w", head=False)
text(s, X0 + 3 * (cw_ + cg), 3.05, 2.6, 0.35, "lost with the old leader", size=11, color=A2)
text(s, R0, 1.75, 4.5, 0.4, "resent oldest first, then new work", size=12, color=A2, bold=True)
text(s, 7.42, 2.2, 0.55, 0.6, "hold", size=12, color=A5, align=C, anchor=M)
for i, (t, d) in enumerate([("Placed is not sequenced", "Each frame is held until the producer's own queue shows it."),
                            ("A term change names the losses", "The newest frames of the old term not yet seen."),
                            ("Limit", "In memory: a restarted producer or promoted standby starts empty; a replaced session drops what was pending.")]):
    x = L + i * (3.84 + 0.305)
    card(s, x, 4.75, 3.84, 1.85, [{"text": t, "bold": True, "size": 15, "space": 6, "color": A2 if i == 2 else TX},
                                  {"text": d, "size": 14}], margin=0.2)


s = content("Failure model", "Crash faults, not Byzantine ones: each failure and its recovery",
            "Walk the table top to bottom. Note what is absent: there is no recovery procedure that is not a replay. "
            "clusterSeqNo, the gateway list and the active instance are all functions of the log. The model is crash "
            "faults. The cluster checks that messages are well formed, not who sent them; authentication belongs at the "
            "system's external edges. A member that cannot publish its queue, or whose log was truncated below what it "
            "applied (an Aeron Cluster 1.53.2 fault), terminates with exit 70 rather than drop a frame or go on from "
            "state the log no longer has. Durability is replication, not fsync: a committed message survives the crash "
            "of any minority of members and of every member process, and can be lost if a majority of hosts lose power "
            "before their kernels write it back. Nothing is authenticated or encrypted; the network is the access control.")
table(s, L, 1.8, [2.9, 4.4, 4.83], [
    ["Failure", "What happens", "Recovery"],
    ["Leader lost", "Election; LeadershipChanged is sequenced", "Next clusterSeqNo; producers resend"],
    ["Member lost", "The others keep quorum", "Restart replays the log, rebuilding its queues"],
    ["Member cannot continue", "It exits (70) rather than drop a frame", "As for a lost member"],
    ["Slow consumer, or a missed message", "Its driver drops it in about 0.2 s; the next frame shows the gap",
     "Resume from the Replayer, holding live frames"],
    ["Gateway instance fails", "Its cluster session closes", "The sequencer designates the standby"],
    ["Gateway loses trust in its view", "A fence: it releases its session and exits", "The standby takes over"],
], size=13, rowh=0.5)
for i, (t, d) in enumerate([("Recovery is one operation. ", "Replay the log; all state downstream of it is a function of it."),
                            ("Durability is replication, not fsync. ", "A majority's power loss can lose committed "
                                                                      "messages; no minority's crash can.")]):
    x = L + i * (5.9 + 0.33)
    card(s, x, 5.55, 5.9, 1.1, [{"runs": [(t, {"bold": True}), (d, {})], "size": 14}], margin=0.2, anchor=M)

# ================================================================= BENCHMARKS
section("Benchmarks")
s = content("Three members on EC2", "Derived queues, 11 and 13 November; every figure is one launch",
            "Three c6in.8xlarge hosts, one per member. The root volume is gp3 at 1,000 MB/s and 4,000 IOPS with even "
            "writeback. The load is 100-byte markers from the direct client, in two ramps from 50k to 909k msg/s in 25% "
            "steps of 10 s, and from 466k and from 909k. 100 bytes is representative: production payloads are "
            "50 to 200 bytes. Latency is timed open-loop from the marker's intended send time until the producer sees its "
            "own frame sequenced, so a stall counts. The "
            "derived runs were made behind the flag replayer.deriveFromLog before the release. A second launch is required "
            "before a result decides anything; there has not been one.")
table(s, L, 1.85, [1.7, 4.95], [
    ["", "Set-up"],
    ["Cluster", "3 × c6in.8xlarge, one member per host"],
    ["Storage", "gp3 root volume, 1,000 MB/s and 4,000 IOPS, even writeback"],
    ["Load", "100-byte markers; two ramps from 50k to 909k msg/s in 25% steps of 10 s; two from 466k; two at 909k"],
    ["Latency", "Open loop: from the intended send time to the producer seeing its own frame sequenced"],
    ["Sampling", "Every 10 ms on the sequencer's counters; every 100 ms on memory"],
], size=13, rowh=0.62)
text(s, L, 6.35, 6.65, 0.4, [{"runs": [("One launch. ", {"bold": True, "color": A2}),
                                       ("A second is required before a result decides anything.", {})]}], size=13)
stat(s, 7.55, 1.85, 2.5, 2.1, "909k", "msg/s held", "saturated at 1.14M; the ceiling was reached", 30)
stat(s, 10.2, 1.85, 2.53, 2.1, "0.23 ms", "p99.9, most steps", "p99 0.12–0.14 ms; four steps from 0.8 to 22 ms", 30)
stat(s, 7.55, 4.1, 2.5, 2.2, "3", "young-GC pauses a leader takes", "2.1–3.3 ms each, in a 210 s ramp", 30)
stat(s, 10.2, 4.1, 2.53, 2.2, "5.3 s", "cold replay", "4,000,000 frames off EBS, on the member", 30)

s = content("Saturation: 909k msg/s held, 1.14M saturates", "100-byte messages, ramps from three starting rates",
            "A ramp from 50k to 909k held every step: the p50 rose from 79 to 104 µs and the p99 from 126 to 140 µs in "
            "the second ramp, which the chart shows. The first ramp had the same curve with four steps worse: 0.8 ms at "
            "909k, 1.4 ms at 727k, 3.3 ms at 122k and 21.7 ms at 582k, where the p99 was 8.0 ms and 488k markers were "
            "declined. Two ramps that started at 466k held 909k and saturated at 1.14M msg/s, where the p50 itself "
            "reached 0.8 and 1.3 s: the offered rate is above what the cluster sequences, and the backlog grows. At 909k "
            "their p99 was 1.5 to 3.6 ms, against 0.14 ms after the full ramp. Two runs that started at 909k carried a "
            "start-up backlog of about a quarter of a second: the leader's commit position ran 13% above the offered rate "
            "for two seconds to drain it, no cycle exceeded 28 ms, and the p99 of 165 and 287 ms is that backlog. The "
            "p50 was 116 and 551 µs. What builds the backlog is not traced.")
chart(s, L, 1.95, 5.9, 3.6, ["50k", "62k", "78k", "98k", "122k", "153k", "191k", "238k", "298k", "373k", "466k",
                              "582k", "728k", "909k"],
      [("p50", [78.8, 86.8, 89.8, 92.1, 93.4, 93.6, 95.1, 98.6, 97.3, 98.7, 99.7, 101.0, 100.4, 104.1]),
       ("p99", [126.4, 124.8, 124.1, 126.1, 128.3, 127.9, 127.7, 129.6, 129.3, 130.5, 131.6, 134.1, 133.3, 140.3])],
      ["accent1", "accent2"], size=11, legend=True, top=200)
text(s, L, 1.62, 5.9, 0.3, "LATENCY IN µs BY OFFERED RATE, A RAMP FROM 50k", size=11, bold=True, color=A5)
table(s, 6.9, 1.85, [1.2, 3.1, 1.53], [
    ["Start", "Result", "p99 at 909k"],
    ["50k", "909k held, every step", "0.14 ms"],
    ["466k", "909k held; saturated at 1.14M", "1.5–3.6 ms"],
    ["909k", "start-up backlog of about 0.25 s", "165–287 ms"],
], size=12, rowh=0.62)
text(s, 6.9, 4.5, 5.83, 0.5, "Two runs from each start; one launch. The chart is the second ramp from 50k.", size=12,
     color=A5)
card(s, L, 5.75, CW, 0.9, [{"runs": [("The ceiling is 1.14M msg/s. ", {"bold": True}),
                                     ("At 1,136,853 the p50 passed 0.8 s. A cold start at 909k has a backlog to drain "
                                      "first; the sequencing path itself is not at its limit there.", {})],
                            "size": 14}], margin=0.2, anchor=M)

s = content("Where the tail comes from", "No regular GC pause; isolated tails remain",
            "A member allocates about 3 MB/s on a leader and no more on a follower, so a young-GC pause is rare: a leader "
            "took three of 2.1 to 3.3 ms in a 210 s ramp, a follower one. A pause on the leader holds every message in "
            "flight; one on a follower is covered by the other. The tails that remain are isolated: in two ramps from "
            "50k, four steps had a p99.9 above 0.8 ms, at 122k, 582k, 727k and 909k. The 21.7 ms at 582k had no young "
            "GC on the leader in its step and 488k declined markers. The 1.4 ms at 727k has one, the leader's third "
            "pause. The others are not traced. A virtualised instance has worse tails than bare metal, so the p99.9 "
            "figures are an upper bound for the same CPU.")
stat(s, L, 1.85, 3.84, 2.4, "3 MB/s", "a leader allocates", "a follower no more; 3 pauses of 2.1–3.3 ms in a 210 s ramp")
stat(s, L + 4.145, 1.85, 3.84, 2.4, "0.23 ms", "p99.9 in most steps",
     "p99 0.12–0.14 ms from the second step to 909k")
stat(s, L + 8.29, 1.85, 3.84, 2.4, "21.7 ms", "the worst p99.9, at 582k",
     "no young GC on the leader in that step; 8.0 ms p99, 488k declined")
card(s, L, 4.55, CW, 2.1, [
    {"text": "A pause on the leader holds every message in flight; one on a follower is covered by the other.",
     "size": 15, "space": 8},
    {"runs": [("Unexplained: ", {"bold": True, "color": A1}),
              ("the steps at 122k, 582k and 909k above 0.8 ms, the worst 21.7 ms at 582k, in two ramps from 50k. "
               "One of the four, at 727k, held the leader's third pause. A second launch would show whether the rest recur.", {})], "size": 15}], margin=0.25, anchor=M)

s = content("Cold replay and stalls", "Replay costs more under load; the disk stalls are gone",
            "Cold replay of 4,000,000 frames of 193 bytes with every member's page cache dropped before each phase, in "
            "the same launch as the saturation runs. A member's archive held 1,152 MB derived and 2,304 MB recorded. "
            "Derived replay is slower under load, 10.8 s against 6.8 s, which is the price of computing it. "
            "Stalls: one c6in.8xlarge held three unpinned members, each archive on its own gp3 volume with "
            "the kernel's default writeback, at 49k msg/s for 600 s, three runs in one launch. Two derived runs and one "
            "recorded: no stall rate, and nothing at a higher rate or a larger payload, where the log alone writes what "
            "both wrote here. The three members share one kernel.")
text(s, L, 1.7, 5.9, 0.35, "COLD REPLAY, 4,000,000 FRAMES OF 193 BYTES", size=11, bold=True, color=A5)
table(s, L, 2.05, [2.7, 1.6, 1.6], [
    ["", "Derived", "Also records"],
    ["Member, quiet", "5.3 s", "5.2 s"],
    ["Member, under load", "10.8 s", "6.8 s"],
    ["Archive on disk", "1,152 MB", "2,304 MB"],
], size=13, rowh=0.55)
text(s, 6.9, 1.7, 5.83, 0.35, "STALLS ON EBS, 600 S AT 49K MSG/S", size=11, bold=True, color=A5)
table(s, 6.9, 2.05, [2.9, 1.45, 1.48], [
    ["", "Derived", "Also records"],
    ["Gaps of 10 ms or more", "0", "17"],
    ["Longest gap", "7 ms", "81 ms"],
    ["Recorder's longest write", "3.4 ms", "79 ms"],
    ["Archive growth", "3.7 GB", "7.3 GB"],
    ["Dirty pages, peak", "619 MB", "1,270 MB"],
], size=13, rowh=0.55)
card(s, L, 5.65, CW, 1.0, [{"runs": [("Two caveats. ", {"bold": True, "color": A2}),
                                     ("The derived stall run was repeated, the recorded one was not; the three members share "
                                      "one host's kernel.", {})],
                            "size": 14}], margin=0.2, anchor=M)

s = content("Below saturation", "Measured on 8 November, on members that also recorded their queues; derived members are on the saturation slide",
            "The latency of the sequencing loop at 20k, 50k and 100k msg/s from the earlier session: members that recorded "
            "their queues, 18-byte payloads, gp3 at its default 125 MB/s, three runs per arm at 20k and eight at 50k and "
            "100k, one launch. The 2 to 3 ms p99.9 from 50k is the size the saturation session measured from the "
            "recorder's writes on that arm. On derived members from 50k to 98k msg/s the p99.9 was 0.13 to 0.23 ms, bar "
            "2.0 and 5.2 ms on the first step of a fresh cluster; the p50 was 79 to 92 µs. Forced archive writes cost 6.5 ms at p50 and 9 ms at p99 with "
            "throughput held, on EBS only. The hosted façade against the direct client: no resolvable difference at p50 "
            "and p99 at these rates with a spinning host; the default idle strategy adds 61 µs at p50. The generator was "
            "co-located with the leader's host; the marker travels over IPC, is replicated to a majority over UDP, and "
            "comes back through the queue.")
text(s, L, 1.7, 6.2, 0.3, "LATENCY IN µs, THE LOOP ONLY", size=11, bold=True, color=A5)
chart(s, L, 2.0, 6.2, 3.7, ["20k msg/s", "50k msg/s", "100k msg/s"],
      [("p50", [37, 63, 78]), ("p99", [49, 110, 380])], ["accent1", "accent2"], size=12, legend=True, top=450)
stat(s, 7.2, 1.85, 5.53, 1.45, "64 µs → 3.3 ms", "p99.9, from 20k to 100k msg/s",
     "2.6 ms at 50k; the 2–3 ms tail is the recorder's writes", 24)
stat(s, 7.2, 3.45, 5.53, 1.45, "+6.5 ms at p50", "with forced archive writes",
     "+9 ms at p99, throughput held; EBS only", 24)
stat(s, 7.2, 5.05, 5.53, 1.45, "No difference", "hosted façade against direct",
     "at p50 and p99 with a spinning host; backing off adds 61 µs", 24)
text(s, L, 5.9, 6.2, 0.7, "Medians over runs. 18-byte payload, default 125 MB/s gp3, one launch.", size=12, color=A5)

s = content("What the benchmarks do not show", "Said here so nobody has to ask",
            "The limits of the sessions. One launch: placement and neighbours vary between launches by "
            "more than between runs, so nothing has been repeated across launches. Derived members' latency below "
            "50k msg/s was not measured. A sequencer exists to merge many producers, and the sweep of 1, 4 and 16 "
            "sessions was not run. The time from a leadership change to the first sequenced command is not measured. "
            "A virtualised instance has worse tails than bare metal, so the p99.9 figures are an upper bound for the same "
            "CPU, and the p50 should carry. Two results are unexplained: the 21.7 ms p99.9 at 582k with no young GC on the "
            "leader, and a p99 of 1.5 to 3.6 ms at 909k after a start at 466k against 0.14 ms after the full ramp. "
            "The raw results are not in the repository.")
cols = [("Not measured", A1, ["A second launch of the same instances", "Derived latency below 50k msg/s",
                              "Many producers: 1, 4 and 16 sessions", "Leadership change to first sequenced command"]),
        ("Unexplained", A2, ["A 21.7 ms p99.9 at 582k with no young GC on the leader",
                             "A p99 of 1.5–3.6 ms at 909k after a start at 466k; 0.14 ms after the full ramp"]),
        ("Caveats", TX, ["A virtualised instance: the p99.9 figures are an upper bound; the p50 should carry",
                         "Raw results are not in the repository",
                         "Standing limits: seven members, crash faults, no wire security, no fsync"])]
for i, (t, c, pts) in enumerate(cols):
    x = L + i * (3.84 + 0.305)
    card(s, x, 1.85, 3.84, 4.3, [{"text": t, "size": 18, "bold": True, "head": True, "color": c, "space": 8}]
         + [{"text": p, "size": 14, "bullet": True, "space": 8} for p in pts], margin=0.22)
text(s, L, 6.3, CW, 0.4, [{"runs": [("Needed: ", {"bold": True, "color": A1}),
                                    ("a second launch, derived latency below 50k msg/s, and a producer sweep.", {})]}],
     size=14)


section("Discussion")

s = prs.slides.add_slide(L_SECTION)
_track(s)
tape(s, 0.8, 2.15, 6, 0)
for ph in s.placeholders:
    tf = ph.text_frame
    tf.margin_left = tf.margin_right = 0
    if ph.placeholder_format.idx == 0:
        tf.vertical_anchor = MSO_ANCHOR.TOP
        fill_tf(tf, [{"text": "Discussion", "size": 44, "bold": True, "color": BG, "head": True}])
    else:
        fill_tf(tf, [{"text": "Questions", "size": 20, "color": A3}])
notes(s, "The last slide presented; the appendix is backup, for questions or a meeting that runs long. Open the "
         "floor. The decisions slide, at the start of the appendix, is the natural place to return to.")


# ================================================================= APPENDIX
section("Appendix")

s = content("Context and alternatives", "The problem it solves, and the options it is weighed against",
            "The alternatives are compared on properties. Kafka on one partition gives a total order and mature "
            "tooling, but no clock that advances while producers are silent and no active/standby producer election. "
            "Aeron without clustering gives reliable UDP and IPC transport with recording and replay, but no "
            "replication: the sequencer is one process and a single point of failure. Aeron Cluster used directly "
            "gives the same consensus and log, but each cluster then rebuilds the queue, replay, the election, confirmed "
            "ingress and the fences; the slide Aeron Cluster used directly is that list. A database sequence is familiar and "
            "transactional, but sequences skip values on rollback, so the order is not gap-free, and every message is "
            "a database write.")
card(s, L, 1.75, CW, 1.1, [
    {"text": "Services that must agree on what happened, and in what order: a trading venue, an order management "
             "system, a risk engine. The sequencer architecture settles the order once, in one place, and every "
             "service becomes a deterministic replica of one stream.", "size": 15}], margin=0.2)
table(s, L, 3.1, [2.6, 4.4, 5.13], [
    ["Option", "Gives", "Drawback"],
    ["Kafka, one partition", "Total order, retention, mature tooling", "No cluster clock or producer election"],
    ["Aeron", "Reliable UDP and IPC; recording, replay", "No replication: one sequencer, one point of failure"],
    ["Aeron Cluster alone", "The same consensus and log", "Replay, election and confirmed ingress rebuilt per cluster"],
    ["Database sequence", "Familiar and transactional", "Skips values on rollback; a write per message"],
], size=14, rowh=0.55)
text(s, L, 6.1, CW, 0.4, [{"runs": [("A total order is one serialization point ", {"bold": True}),
                                     ("in every option, seqeron's included: its throughput ceiling is the same in "
                                      "kind.", {})]}], size=14, color=A5)


s = content("Aeron Cluster", "Raft over Aeron's transport and archive: replicated log, deterministic services",
            "Each member runs a consensus module, an archive and a clustered service container. A client opens a "
            "session and sends on ingress to the leader; responses come back on egress. The leader appends each message "
            "to the Raft log, a stream every member's archive records; followers report append positions and an entry "
            "commits at a majority. Every cluster member's service consumes committed entries in the same order. Session "
            "events, timers and the leader's timestamp travel in the log, so time is deterministic too. A restarting "
            "member normally loads a snapshot and replays the log after it; seqeron takes cluster snapshots only under a snapshots policy, as the Decisions slide covers.")
f = Fig(s, L, 1.75, CW / 960)
f.host(10, 120, 170, 110, "Cluster client")
f.label(10, 160, 170, 24, "AeronCluster", size=12, color=TX, bold=True)
f.label(10, 186, 170, 24, "one session")
f.host(240, 20, 330, 340, "Leader", align=PP_ALIGN.LEFT)
f.box(260, 60, 290, 60, "Consensus module", "election · append · commit · cluster time")
f.box(260, 150, 130, 60, "Archive", "records the log")
f.box(410, 150, 140, 60, "Clustered service", "applies committed log")
f.arrow([(325, 120), (325, 150)])
f.arrow([(480, 120), (480, 150)])
f.label(258, 222, 250, 22, "commit = a majority has appended", align=PP_ALIGN.LEFT)
f.label(258, 244, 250, 34, "timers, session events and timestamps ride the log", align=PP_ALIGN.LEFT)
f.arrow([(180, 160), (260, 95)], "a")
f.label(150, 112, 80, 20, "ingress", color=A1)
f.arrow([(530, 210), (530, 300), (95, 300), (95, 230)], "d")
f.label(150, 304, 300, 22, "egress: responses and session events")
for oy in (20, 200):
    f.host(630, oy, 320, 160, "Follower", align=PP_ALIGN.LEFT)
    f.box(650, oy + 35, 130, 46, "Consensus module")
    f.box(800, oy + 35, 130, 46, "Archive")
    f.box(650, oy + 95, 280, 46, "Clustered service", "same input, same state")
f.arrow([(550, 82), (650, 78)], "a")
f.arrow([(550, 96), (650, 258)], "a")
f.label(560, 52, 80, 20, "log", color=A1)
text(s, L, 6.4, CW, 0.4, [{"runs": [("Member: ", {"bold": True}),
                                    ("one node of the cluster: consensus module, archive, clustered service. In this deck, "
                                     "always a cluster member.", {})]}], size=14)


s = content("The gateway election", "Every input comes from the log; the output is a sequenced frame",
            "Walk the tape left to right. The topology rows arrive; the last one has remaining 0, which is the "
            "completeness edge, and the sequencer synthesizes GatewayActive for each rank-0 instance. GW-A announces "
            "itself with GatewayStarted, which binds its cluster session, and opens connections. When A's session "
            "closes, through a crash, network loss or shutdown, the sequencer designates B. B's GatewayStarted carries a "
            "first connection id above the highest it saw, and a consumer treats it as closing every connection still "
            "open under the pair, since a crashed instance never closes them. B rebuilt A's connections from the log while "
            "it was a hot standby.")
sc = CW / 980
f = Fig(s, L - 10 * sc, 1.75, sc)
f.label(10, 0, 400, 26, "log, in clusterSeqNo order →", size=12, align=PP_ALIGN.LEFT)
tape_frames = [("GatewayRegistered", "GW-A · rank 0", "box"), ("GatewayRegistered", "GW-B · rank 1 · last", "box"),
               ("GatewayActive", "GW-A · bootstrap", "syn"), ("GatewayStarted", "GW-A · binds session", "box"),
               ("ConnectionOpened", "ids 1 … n, by GW-A", "box"), ("GatewayActive", "GW-B · A's session closed", "syn"),
               ("GatewayStarted", "GW-B · first id > n", "box")]
for k, (t, d, st) in enumerate(tape_frames):
    f.box(10 + k * 138, 34, 128, 82, t, d, st, tsize=11, ssize=10)
f.label(10, 140, 60, 30, "GW-A", size=13, color=TX, align=PP_ALIGN.LEFT, bold=True)
f.box(424, 142, 276, 26, "serving", style="bar", tsize=11, tcolor=BG)
f.label(706, 140, 200, 30, "crashed: session closes", size=11, align=PP_ALIGN.LEFT)
f.label(10, 190, 60, 30, "GW-B", size=13, color=TX, align=PP_ALIGN.LEFT, bold=True)
f.box(70, 192, 630, 26, None, style="dim")
f.label(80, 190, 600, 30, "hot standby: follows the queue, rebuilds A's connections from the log", size=11, color=TX,
        align=PP_ALIGN.LEFT)
f.box(838, 192, 128, 26, "serving", style="bar", tsize=11, tcolor=BG)
y0 = 1.75 + 240 * sc
shape(s, L, y0 + 0.1, 0.3, 0.3, "syn", name="Legend")
text(s, L + 0.4, y0 + 0.05, 5, 0.4, "synthesized by the sequencer", size=12, color=A5, anchor=M)
shape(s, L + 4.2, y0 + 0.1, 0.3, 0.3, "box", name="Legend")
text(s, L + 4.6, y0 + 0.05, 6, 0.4, "submitted by clusterctl or an instance", size=12, color=A5, anchor=M)
card(s, L, y0 + 0.7, CW, 0.95, [{"runs": [("Every gateway instance and every application sees the same decision at the same clusterSeqNo. ", {"bold": True}),
                                          ("No two instances can act as active on inconsistent information.", {})], "size": 15}],
     margin=0.22, anchor=M)


s = content("Leader-only work, through the log", "Outstanding work is replicated; dispatch is local",
            "Every application instance records a request as outstanding from its sequenced request until its sequenced reply. Only "
            "the leader's instance dispatches; it replies under the requester's sourceId and connectionId so the "
            "originating gateway can route it. The request is discharged when the reply is sequenced, not when it is "
            "sent. Every LeadershipChanged closes the gate, including one naming the same member, because a reply sent "
            "during the election may have been lost with the old leader's uncommitted log. A newly opened gate "
            "dispatches every outstanding request in clusterSeqNo order. Delivery is at least once across a failover; a "
            "reply that is a pure function of its request is byte-identical on re-emission and deduplicated by key.")
sc = CW / 960
f = Fig(s, L, 1.7, sc)
f.host(10, 120, 120, 50, "External")
f.box(170, 120, 150, 50, "Gateway", "active instance")
f.arrow([(130, 138), (170, 138)])
f.arrow([(170, 154), (130, 154)])
f.box(360, 120, 110, 50, "seq N", "request", "queue")
f.arrow([(320, 145), (360, 145)])
f.box(510, 40, 180, 50, "Application · member 0", "outstanding: N", tsize=11)
f.box(510, 120, 180, 50, "Application · member 1", "leader: reply(requester, conn)", "lead", tsize=11)
f.box(510, 200, 180, 50, "Application · member 2", "outstanding: N", tsize=11)
for ty in (65, 145, 225):
    f.arrow([(470, 145), (510, ty)], "d")
f.box(730, 120, 110, 50, "seq M", "reply", "queue")
f.arrow([(690, 145), (730, 145)], "a")
f.label(845, 112, 115, 66, "every instance: N discharged", size=11, align=PP_ALIGN.LEFT)
f.arrow([(785, 170), (785, 278), (245, 278), (245, 170)], "a")
f.label(300, 280, 440, 24, "gateway keeps its own sourceId, routes by connectionId", size=11)
pts3 = [("Replicated", "Outstanding from sequenced request to sequenced reply, on every instance."),
        ("Gate closes", "On every LeadershipChanged, even for the same leader, and at a fence."),
        ("At least once", "Deterministic replies make duplicates identical; drop by key.")]
for i, (t, d) in enumerate(pts3):
    x = L + i * (3.84 + 0.305)
    card(s, x, 5.7, 3.84, 1.05, [{"runs": [(t + "  ", {"bold": True}), (d, {})], "size": 13}], margin=0.15, anchor=M)


s = content("Application snapshots", "Local files the log confirms; no cluster snapshots without a policy",
            "The sequencer sequences SnapshotStarted as a round's cut, on an operator request or the policy interval. "
            "Every instance of a participating source serializes its state at that cut into its own local file. The "
            "instance that may publish submits SnapshotEnd with record count, length and CRC32C. Every instance compares "
            "its own file with the sequenced end and fences on a difference, which also makes each round a divergence "
            "check across instances; a match confirms the file, which replaces every older one. A restarting instance "
            "restores its confirmed file once a replay from the file's cut position opens on that cut, and replays only "
            "what follows; with no such file it replays from 1. The Replayer holds no snapshot state. "
            "This shortens an application's restart, not a member's.")
flow = [("Begin", "Sequencer sequences SnapshotStarted"), ("Serialize", "Every instance writes its own local file"),
        ("End", "The publishing instance submits SnapshotEnd: count, length, CRC32C"),
        ("Compare", "Every instance checks its file: a match confirms it, a difference fences"),
        ("Restore", "Newest confirmed file, then only what follows the cut")]
fw = (CW - 4 * 0.35) / 5
for i, (t, d) in enumerate(flow):
    x = L + i * (fw + 0.35)
    shape(s, x, 2.0, fw, 2.9, "syn" if i == 0 else "card", name="Step")
    num_circle(s, x + 0.2, 2.2, i + 1, d=0.45, size=14, style="warm" if i == 0 else "accent")
    text(s, x + 0.2, 2.8, fw - 0.4, 0.45, t, size=18, bold=True, head=True)
    text(s, x + 0.2, 3.3, fw - 0.4, 1.5, d, size=14)
    if i < 4:
        arrow(s, [(x + fw, 3.45), (x + fw + 0.35, 3.45)], "a")
card(s, L, 5.3, CW, 1.3, [{"text": "Application snapshots bound a client's restart to the messages since the last one "
                                   "and are the intended boundary for application version upgrades.", "size": 16}],
     margin=0.25, anchor=M)


s = content("Decisions and what they cost", "The choices worth challenging",
            "Lead the discussion here. The most consequential is the Raft log as the only recording: it halves the "
            "archive's writes and removes the recorder's tail, and costs replay rate, since a replay is computed. The "
            "single stream is the other: one order for everything with no coordination, at the price of one sequencer's "
            "throughput for all producers. A cluster with no snapshots policy takes none, which suits a deployment that "
            "resets its archive between runs.")
table(s, L, 1.8, [3.75, 4.25, 4.13], [
    ["Decision", "Buys", "Costs"],
    ["The Raft log is the only recording", "Half the archive writes; one copy of history",
     "Replay is computed: a third to a half of an archive's rate"],
    ["Untethered consumers", "Nobody slows the sequencer", "A slow consumer pays a replay"],
    ["A member exits (70) rather than drop a frame", "Never serves history it lacks", "Costs a member, not a frame"],
    ["Opaque payloads", "Applications version alone", "No content routing or checks"],
    ["One ordered stream", "No coordination anywhere", "One sequencer's throughput"],
    ["Majority replication, no fsync", "Memory and network latency", "A majority's power loss can lose messages"],
    ["No cluster snapshots by default", "Complete history on every member", "Recovery and archive grow with uptime"],
], size=14, rowh=0.56)

s = content("Limits today", "Stated plainly, so nobody discovers them in production",
            "Seven members is bounded by the 70-port cluster block. Payloads above 8,874 bytes are split by the "
            "application, which keeps a frame within one IPC MTU. Four concurrent replays per member; more wait. "
            "Nothing on the wire is authenticated or encrypted: the network is the access control, so the port block "
            "belongs inside a trusted network. The benchmarks are one launch on EC2. A change to seqeron's own protocol "
            "moves every member, gateway host and client together on purged archives; applications upgrade "
            "independently.")
lims = [("7", "members at most", "bounded by the 70-port cluster block"),
        ("8,874 B", "payload", "larger payloads are split by the application"),
        ("4", "replays per member at once", "more wait"),
        ("None", "wire security", "no authentication or encryption; the network is the access control"),
        ("1 launch", "benchmarks", "909k msg/s of 100 bytes on EC2; a second launch is required"),
        ("All at once", "protocol upgrade", "seqeron's own protocol, on purged archives")]
for i, (big, lab, d) in enumerate(lims):
    col, row = i % 3, i // 3
    x, y = L + col * (3.84 + 0.305), 1.85 + row * (2.3 + 0.25)
    card(s, x, y, 3.84, 2.3, [{"text": big, "size": 24, "bold": True, "color": A1, "head": True, "space": 4},
                              {"text": lab, "size": 15, "bold": True, "space": 4},
                              {"text": d, "size": 14, "color": A5}], margin=0.22)


# ---------------------------------------------------------------- sections
if "sections" in OFF: SECTIONS.clear()
P14 = "http://schemas.microsoft.com/office/powerpoint/2010/main"
pres_el = prs.part._element
ext_lst = pres_el.find(qn("p:extLst"))
if ext_lst is None:
    ext_lst = etree.SubElement(pres_el, qn("p:extLst"))
ext = etree.SubElement(ext_lst, qn("p:ext"))
ext.set("uri", "{521415D9-36F7-43E2-AB2F-B90AF26B5E84}")
sl = etree.SubElement(ext, f"{{{P14}}}sectionLst", nsmap={"p14": P14})
for name, ids in SECTIONS:
    sec = etree.SubElement(sl, f"{{{P14}}}section")
    sec.set("name", name)
    sec.set("id", "{" + str(uuid.uuid4()).upper() + "}")
    lst = etree.SubElement(sec, f"{{{P14}}}sldIdLst")
    for sid in ids:
        etree.SubElement(lst, f"{{{P14}}}sldId").set("id", str(sid))

# python-pptx relates the notes master but never lists it; Keynote refuses such a package.
nm_rid = next(r.rId for r in prs.part.rels.values() if r.reltype == RT.NOTES_MASTER)
if pres_el.find(qn("p:notesMasterIdLst")) is None:
    nml = etree.Element(qn("p:notesMasterIdLst"))
    etree.SubElement(nml, qn("p:notesMasterId")).set(qn("r:id"), nm_rid)
    pres_el.find(qn("p:sldMasterIdLst")).addnext(nml)

prs.core_properties.title ="seqeron architecture and benchmarks"
prs.core_properties.author = "Fredrik Dahlberg"
prs.save(OUT)
print("slides:", len(prs.slides))

