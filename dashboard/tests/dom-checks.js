// Layout and accessibility assertions for the built dashboard, evaluated IN THE PAGE (chrome-devtools
// evaluate_script, or paste into the console) against the fixture server. Visits every tab and returns
// only failures:
//   overflow  - the page scrolls sideways
//   spills    - an element sticks out of its card / ticket / alert / tile (tables scroll inside
//               .table-scroll, which is allowed and reported separately as `scrollers`)
//   small     - an interactive target under 24 x 24 px (WCAG 2.2 2.5.8 floor)
//   contrast  - text under 4.5:1 (3:1 for large text) against its real composited background
// Not a unit test: `node --test` ignores this file (the glob is tests/*.test.ts).
async () => {
  const TABS = ['overview', 'recommendations', 'paper', 'performance', 'activity', 'system']
  const parse = (s) => {
    if (!s) return null
    let m = /^rgba?\(([^)]+)\)$/.exec(s)
    if (m) {
      const p = m[1].split(/[\s,/]+/).filter(Boolean).map(Number)
      return { r: p[0], g: p[1], b: p[2], a: p.length > 3 ? p[3] : 1 }
    }
    m = /^color\(srgb ([^)]+)\)$/.exec(s)
    if (m) {
      const p = m[1].split(/[\s/]+/).filter(Boolean).map(Number)
      return { r: p[0] * 255, g: p[1] * 255, b: p[2] * 255, a: p.length > 3 ? p[3] : 1 }
    }
    return null
  }
  const lin = (c) => {
    c /= 255
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
  }
  const lum = (c) => 0.2126 * lin(c.r) + 0.7152 * lin(c.g) + 0.0722 * lin(c.b)
  const ratio = (a, b) => {
    const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p)
    return (x + 0.05) / (y + 0.05)
  }
  const over = (t, u) => ({ r: t.r * t.a + u.r * (1 - t.a), g: t.g * t.a + u.g * (1 - t.a), b: t.b * t.a + u.b * (1 - t.a), a: 1 })
  const bgOf = (el) => {
    const layers = []
    for (let n = el; n; n = n.parentElement) {
      const c = parse(getComputedStyle(n).backgroundColor)
      if (c && c.a > 0) {
        layers.push(c)
        if (c.a >= 1) break
      }
    }
    let acc = { r: 11, g: 15, b: 21, a: 1 }
    for (let i = layers.length - 1; i >= 0; i--) acc = over(layers[i], acc)
    return acc
  }
  const visible = (el) => {
    if (el.closest('.sr-only, [aria-hidden="true"]')) return false
    const cs = getComputedStyle(el)
    return cs.visibility !== 'hidden' && cs.display !== 'none' && el.getClientRects().length > 0
  }
  const name = (el) => `${el.tagName.toLowerCase()}${el.className && typeof el.className === 'string' ? '.' + el.className.split(' ')[0] : ''} "${(el.textContent || el.getAttribute('aria-label') || '').trim().slice(0, 32)}"`

  const results = []
  for (const tab of TABS) {
    location.hash = tab
    await new Promise((r) => setTimeout(r, 350))
    const vw = document.documentElement.clientWidth
    const out = { tab, overflow: document.documentElement.scrollWidth > vw + 1, spills: [], scrollers: [], small: [], contrast: [] }

    document.querySelectorAll('.table-scroll').forEach((t) => {
      if (t.scrollWidth > t.clientWidth + 1) out.scrollers.push(`${t.getAttribute('aria-label')}: ${t.scrollWidth}/${t.clientWidth}`)
    })
    document.querySelectorAll('.card, .ticket, .alert, .stat, .today-link').forEach((box) => {
      const br = box.getBoundingClientRect()
      box.querySelectorAll('*').forEach((el) => {
        const sc = el.closest('.table-scroll')
        if (sc && box.contains(sc)) return
        const r = el.getBoundingClientRect()
        if (!r.width || !r.height || !visible(el)) return
        if (r.right > br.right + 1 || r.left < br.left - 1) out.spills.push(name(el))
      })
    })
    document.querySelectorAll('button, [role="tab"], [role="switch"], input, select, summary, a[href]').forEach((el) => {
      if (!visible(el)) return
      const r = el.getBoundingClientRect()
      if (r.height < 23.5 || r.width < 23.5) out.small.push(`${name(el)} ${Math.round(r.width)}x${Math.round(r.height)}`)
    })
    document.querySelectorAll('body *').forEach((el) => {
      const own = [...el.childNodes].some((n) => n.nodeType === 3 && n.textContent.trim())
      if (!own || !visible(el) || el.closest(':disabled, [aria-disabled="true"]')) return
      const cs = getComputedStyle(el)
      const fg = parse(el instanceof SVGElement ? cs.fill : cs.color)
      if (!fg) return
      const bg = bgOf(el)
      const c = ratio(fg.a < 1 ? over(fg, bg) : fg, bg)
      const size = parseFloat(cs.fontSize)
      const large = size >= 24 || (size >= 18.66 && Number(cs.fontWeight) >= 700)
      if (c < (large ? 3 : 4.5)) out.contrast.push(`${name(el)} ${c.toFixed(2)}`)
    })
    out.spills = [...new Set(out.spills)].slice(0, 8)
    out.small = [...new Set(out.small)].slice(0, 8)
    out.contrast = [...new Set(out.contrast)].slice(0, 8)
    results.push(out)
  }
  location.hash = 'overview'
  return results.filter((r) => r.overflow || r.spills.length || r.scrollers.length || r.small.length || r.contrast.length)
}
