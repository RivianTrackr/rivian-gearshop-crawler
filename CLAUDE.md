## Design Language

A hand-crafted CSS design system with two theme modes (dark consumer-facing,
light admin/internal). Pure CSS custom properties — no Tailwind, no preprocessor,
no component library. Icons via Font Awesome 6. Modern CSS features assumed
(`color-mix()`, `backdrop-filter`, `:has()`, `dvh` units, `env()` safe-area insets).

### Color Primitives

**Dark theme (consumer-facing)**

| Role            | Value     | Notes                          |
|-----------------|-----------|--------------------------------|
| Background      | `#121e2b` | Primary surfaces, cards        |
| Background deep | `#0f1a26` | Deepest layer, inset regions   |
| Input surface   | `#374151` | Form controls, select fields   |
| Text primary    | `#e5e7eb` | Body copy, headings            |
| Border          | `#374151` | Dividers, card edges           |
| Accent          | `#fba919` | CTAs, links, focus rings, stars|
| Secondary CTA   | `#7c3aed` | Alternate action (hover: `#6d28d9`) |
| Destructive     | `#ef4444` | Delete, favorite active, error |
| Positive        | `#4ade80` | User-contributed highlights    |
| Star empty      | `#2d3a49` | Unfilled rating slots          |
| Placeholder     | `#9ca3af` | Input placeholder text         |
| Overlay dark    | `#0f172a` | Text-on-accent, dark contrast  |

**Light theme (admin/internal)**

| Role             | Value     | Notes                          |
|------------------|-----------|--------------------------------|
| Background       | `#ffffff` | Page & card surface            |
| Surface          | `#f5f5f7` | Subtle backgrounds, table head |
| Surface hover    | `#e8e8ed` | Row hover, button hover        |
| Text primary     | `#1d1d1f` | Headings, body                 |
| Text secondary   | `#6e6e73` | Descriptions, metadata         |
| Text muted       | `#86868b` | Placeholders, disabled         |
| Border           | `#d2d2d7` | Inputs, cards, dividers        |
| Action           | `#0071e3` | Primary buttons, links         |
| Action hover     | `#0077ed` | Hover state                    |
| Row hover        | `#fafafa` | Table row highlight            |

**Semantic status colors (both themes)**

| Status  | Fill        | Background  | Text       |
|---------|-------------|-------------|------------|
| Success | `#34c759`   | `#d1f4e0`   | `#0a5e2a`  |
| Error   | `#ff3b30`   | `#ffe5e5`   | `#c41e3a`  |
| Warning | —           | `#fff3cd`   | `#856404`  |
| Info    | `#60a5fa`   | `#dbeafe`   | `#1e40af`  |

**Grade scale (A–F data visualization)**

| Grade | Color     | Text      |
|-------|-----------|-----------|
| A     | `#34c759` | white     |
| B     | `#7dc734` | white     |
| C     | `#facc15` | `#1d1d1f` |
| D     | `#f97316` | white     |
| F     | `#b91c1c` | white     |

**OEM/featured gradient:** `linear-gradient(135deg, #047857, #10b981, #34d399)`

### Typography

```
Sans:  -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen-Sans,
       Ubuntu, Cantarell, "Helvetica Neue", sans-serif
Mono:  'SF Mono', Monaco, 'Cascadia Code', 'Roboto Mono', Consolas,
       'Courier New', monospace
```

| Scale           | Size   | Weight | Extras                                |
|-----------------|--------|--------|---------------------------------------|
| Page title      | 32px   | 600    | line-height 1.2                       |
| Stat value      | 28px   | 600    | line-height 1.2, tabular-nums        |
| Section title   | 20px   | 600–700| —                                     |
| Modal title     | 18px   | 700    | —                                     |
| Subhead         | 16px   | 600–700| —                                     |
| Field label     | 15px   | 500    | —                                     |
| Body            | 14–15px| 400–500| line-height 1.4–1.6                   |
| Small / meta    | 13px   | 500–600| —                                     |
| Badge / caption | 12px   | 600    | —                                     |
| Section label   | 11px   | 600–700| uppercase, letter-spacing 0.3–0.8px   |
| Micro badge     | 9px    | 700    | uppercase, letter-spacing 0.5px       |

Antialiasing: `-webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale`

### Spacing

**Base increments used:** 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 24, 32, 40, 60px

| Context                    | Value           |
|----------------------------|-----------------|
| Card padding (mobile)      | 12–16px         |
| Card padding (desktop)     | 20–24px         |
| Card header padding        | 20px 24px       |
| Card gap (between items)   | 12px            |
| Grid gap                   | 16–20px         |
| Section margin-bottom      | 20–24px         |
| Form field vertical rhythm | 20px padding-block |
| Modal padding              | 16–24px         |
| Toast padding              | 14px 20px       |
| Inline element gap         | 6–8px           |
| Action button padding      | 8–16px vert, 16–24px horiz |
| CTA button padding         | 10px 18px       |
| Compact cell padding       | 8–12px          |

### Border Radius

| Token   | Value    | Usage                              |
|---------|----------|------------------------------------|
| `xs`    | `4px`    | Tags, pills, small code blocks     |
| `sm`    | `6px`    | Badges, small buttons, review pills|
| `md`    | `8px`    | Buttons, inputs, standard elements |
| `lg`    | `10px`   | Filter sections, callouts, toasts  |
| `xl`    | `12px`   | Cards, modals, floating bars       |
| `2xl`   | `16px`   | Full modals, chips (pill-shaped)   |
| `pill`  | `20px`   | Filter chips, rounded pill shapes  |
| `toggle`| `34px`   | iOS-style toggle tracks            |
| `full`  | `9999px` | Range tracks, fully rounded        |
| `circle`| `50%`    | Avatars, status icons, rank badges |

### Shadows

| Name              | Value                                        | Usage                      |
|-------------------|----------------------------------------------|----------------------------|
| Toggle dot        | `0 1px 3px rgba(0,0,0,0.15)`                | Toggle switch knob         |
| Subtle            | `0 1px 3px rgba(0,0,0,0.2)`                 | Range thumb, small elevate |
| Tooltip           | `0 4px 12px rgba(0,0,0,0.4)`                | Tooltip popover            |
| Dropdown          | `0 8px 24px rgba(0,0,0,0.3)`                | Image modal                |
| Floating          | `0 8px 25px rgba(0,0,0,0.4)`                | Sticky bottom bar          |
| Toast             | `0 8px 30px rgba(0,0,0,0.4)`                | Toast notifications        |
| Modal             | `0 25px 60px rgba(0,0,0,0.5)`               | Centered modals            |
| Drawer            | `-10px 0 40px rgba(0,0,0,0.4)`              | Side drawer panel          |
| Focus ring (light)| `0 0 0 3px rgba(0,113,227,0.1)`             | Input focus (light theme)  |
| Focus ring (dark) | `0 0 0 2px [accent]`                         | Input focus (dark theme)   |
| Star glow         | `drop-shadow(0 1px 3px color-mix(in srgb, [color] 35%, transparent))` | Filled rating stars |

### Overlays & Backdrop

| Context         | Background                     | Backdrop filter |
|-----------------|--------------------------------|-----------------|
| Modal overlay   | `rgba(0, 0, 0, 0.6)`          | `blur(4px)`     |
| Drawer overlay  | `rgba(0, 0, 0, 0.5)`          | `blur(4px)`     |
| Image lightbox  | `rgba(0, 0, 0, 0.75)`         | `blur(4px)`     |
| Floating bar    | card bg + border               | `blur(8px)`     |
| Icon overlay    | `rgba(15, 23, 42, 0.55)`      | `blur(4px)`     |
| Icon overlay hover | `rgba(15, 23, 42, 0.75)`   | —               |

### Breakpoints

| Name         | Query                 | Behavior                           |
|--------------|-----------------------|------------------------------------|
| Small mobile | `max-width: 480px`    | Stack all, full-width buttons      |
| Mobile       | `max-width: 600px`    | Single column, stacked filters     |
| Mobile modal | `max-width: 640px`    | Full-screen sheet, safe-area inset |
| Tablet       | `max-width: 782px`    | Reduced padding, 2-col stats grid  |
| Desktop      | `min-width: 1024px`   | Full grid layouts (3-col cards)    |
| Wide         | `max-width: 1200px`   | Table cell adjustments             |

Grid columns: `repeat(auto-fill, minmax(300px, 1fr))` general, `repeat(auto-fit, minmax(180px, 1fr))` for stat cards.

### Transitions & Animations

**Duration scale:**

| Token    | Duration | Easing                          | Usage                              |
|----------|----------|----------------------------------|------------------------------------|
| `press`  | 0.1s     | `ease`                           | Active scale (`scale(0.93)`)       |
| `fast`   | 0.15s    | `ease`                           | Hover borders, color changes       |
| `base`   | 0.2s     | `ease`                           | Buttons, inputs, backgrounds       |
| `medium` | 0.25s    | `ease`                           | Chevron rotation, collapse         |
| `slow`   | 0.3s     | `ease`                           | Drawers, modals, max-height toggle |
| `sheet`  | 0.3s     | `cubic-bezier(0.32, 0.72, 0, 1)`| Mobile bottom sheet entry          |
| `bar`    | 0.6s     | `ease`                           | Data bar fill width                |

**Named animations:**

| Animation  | Spec                                  | Usage                        |
|------------|---------------------------------------|------------------------------|
| `spin`     | `0.8s linear infinite`                | Loading spinner (rotate 360) |
| `shimmer`  | `1.5s ease-in-out infinite`           | Skeleton loading placeholder |
| `pulse`    | `2s ease-in-out infinite`             | Badge attention glow         |
| `chipIn`   | `0.2s ease` scale(0.9→1) + fade      | Filter chip entry            |
| `fadeIn`   | opacity 0→1                           | Generic reveal               |
| `slideUp`  | translateY(20px)→0 + fade             | Modal/toast entry            |
| `highlight`| `2s ease` box-shadow accent glow      | Scroll-to target pulse       |

### Component Patterns

**Card** — `border: 1px solid [border]`, `border-radius: 12px`, `overflow: hidden`.
Header/body split with border-bottom divider. Hover: border lightens toward accent via `color-mix()`.

**Button sizes** — Standard height 40–44px. Inline-flex, center-aligned, gap 6px.
Primary: accent bg / dark text. Secondary: transparent + border. Danger: error bg / white.
Press: `transform: scale(0.93)`. Disabled: `opacity: 0.5; cursor: not-allowed`.

**Toggle (iOS-style)** — Two sizes:
- Large (admin): 52×32px track, 28px knob, 2px inset
- Small (consumer): 44×24px track, 18px knob, 3px inset
- Off: muted bg. On: success green. Knob slides `translateX(20px)`.

**Segmented control** — Flex container, `gap: 0`, deep bg, 4px padding, 10px radius.
Child buttons share radius. Active: accent bg, dark text.

**Chip / pill** — `border-radius: 20px`, accent-tinted bg via `color-mix(in srgb, [accent] 12%, [deep-bg])`.
Dismiss button: 18px circle, border bg, hover red `#ee383a`.

**Rating stars** — SVG layers (bg/fill/half). Empty: muted color at 0.35 opacity.
Filled: accent + drop-shadow. Hover: `scale(1.15)`. Secondary color for user-contributed.

**Modal** — Centered, max-width 520px, 16px border-radius, slide-up 20px entry.
Mobile <640px: full-screen bottom sheet via `translateY(100%)`, 0px radius, safe-area padding,
sticky header/footer, `cubic-bezier(0.32, 0.72, 0, 1)` entry.

**Drawer** — Right-aligned, max-width 480px, `translateX(100%)` entry, border-left.
Mobile: full width.

**Toast** — Fixed top-right (24px inset), max-width 380px, slide-in from right,
10px radius, elevated shadow. Status border-color indicates type.

**Skeleton** — Shimmer gradient:
`linear-gradient(90deg, [card-bg] 0%, [muted 8%] 40%, [muted 12%] 50%, [muted 8%] 60%, [card-bg] 100%)`
background-size 200%, animated position sweep 1.5s.

**Table** — Full-width collapse. Header: surface bg, uppercase 13px muted text.
Cells: 12–16px padding, border-bottom divider. Row hover: `#fafafa`.
Row actions: invisible by default, fade in on row hover (0.15s opacity).
Compact variant: 10–12px padding, 12–13px font.

**Tooltip** — 320px max-width, 8px radius, dark bg, elevated shadow.
Hover/focus-within toggles visibility + opacity 0.2s.

**Range slider** — Custom thumb: 20px white circle, 2px accent border.
Track: 6px height, full-round, gradient fill. Hover: `scale(1.15)` + accent ring via `color-mix()`.

**Empty state** — Centered, 60px padding, large muted icon (48–64px),
20px bold heading, 14px description (max-width 500px, line-height 1.6).

### Accessibility

- Focus-visible: `outline: 2px solid [accent]; outline-offset: 2px`
- `prefers-reduced-motion: reduce`: disable all animations, shimmer shows static tint
- Screen-reader-only: standard clip-path pattern
- Safe-area insets: `env(safe-area-inset-top)` and `env(safe-area-inset-bottom)` on mobile sheets
- Print: hide interactive elements, switch to high-contrast paper colors
- Minimum touch targets: 40–44px height
- Checkbox accent-color matches theme action color

### Conventions

- `color-mix(in srgb, [color] N%, transparent)` for tinted backgrounds & borders
- `color-mix(in srgb, [accent] N%, [deep-bg])` for subtle accent surfaces
- Uppercase labels: 11px, weight 600–700, `letter-spacing: 0.3–0.8px`
- Monospace values: use mono stack for numeric data cells
- Icons: Font Awesome 6 solid (`fa-solid` prefix)
- No framework dependencies — all components are vanilla CSS + semantic HTML
- Build tooling: esbuild
