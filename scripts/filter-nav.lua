-- filter-nav.lua
-- Pandoc Lua filter to strip navigation links and clean up
-- markdown for PDF/print output.
--
-- Removes paragraphs that contain only navigation links such as:
--   [← Back to Index](../index.md)
--   [Previous: Chapter N](...)
--   [Next: Chapter N](...)

-- Patterns that indicate a navigation-only paragraph
local nav_patterns = {
  "^%[← Back to",
  "^%[←%s+Back",
  "^%[Previous:",
  "^%[Next:",
  "^← Back to",
  "^Previous:.*|.*Next:",
}

--- Check if a string matches any navigation pattern
local function is_nav_text(text)
  for _, pat in ipairs(nav_patterns) do
    if text:match(pat) then
      return true
    end
  end
  return false
end

--- Stringify an inline list to plain text for pattern matching
local function stringify_inlines(inlines)
  local result = {}
  for _, inline in ipairs(inlines) do
    if inline.t == "Str" then
      result[#result + 1] = inline.text
    elseif inline.t == "Space" or inline.t == "SoftBreak" then
      result[#result + 1] = " "
    elseif inline.t == "Link" then
      -- Stringify the link content
      result[#result + 1] = pandoc.utils.stringify(inline.content)
    elseif inline.t == "Emph" or inline.t == "Strong" then
      result[#result + 1] = pandoc.utils.stringify(inline.content)
    end
  end
  return table.concat(result)
end

--- Filter: Remove navigation paragraphs
function Para(el)
  local text = stringify_inlines(el.content)
  text = text:match("^%s*(.-)%s*$") or text  -- trim whitespace

  if is_nav_text(text) then
    return {} -- remove this paragraph
  end

  return el
end

--- Filter: Remove horizontal rules that commonly appear around nav links
-- We keep HorizontalRule in general but could selectively remove them
-- if needed. For now, keep them as chapter separators.

--- Filter: Fix relative links for chapter cross-references
-- Convert chapter links from relative paths to internal anchors
function Link(el)
  local target = el.target

  -- Strip ../index.md links (they point to the TOC which is already in the PDF)
  if target:match("%.%./index%.md") then
    return el.content  -- return just the link text, no link
  end

  -- Convert relative chapter links like ../chapters/05-openlineage-standard.md
  -- or chapters/05-openlineage-standard.md to internal references
  if target:match("chapters/") then
    -- Keep as-is for now; internal PDF links won't resolve but the text remains
    return el
  end

  return el
end

-- Emoji → text replacements for XeLaTeX compatibility
local emoji_map = {
  ["✅"] = "[YES]",
  ["❌"] = "[NO]",
  ["⚠️"] = "[WARN]",
  ["⚠"] = "[WARN]",
  ["✓"] = "[YES]",
  ["✗"] = "[NO]",
  ["📌"] = "[NOTE]",
  ["💡"] = "[TIP]",
  ["🔑"] = "[KEY]",
  ["📝"] = "[NOTE]",
  ["🚀"] = "[GO]",
  ["📊"] = "[CHART]",
  ["🔗"] = "[LINK]",
  ["⬆️"] = "[UP]",
  ["⬇️"] = "[DOWN]",
  ["➡️"] = "->",
  ["⭐"] = "[*]",
  ["🎯"] = "[TARGET]",
  ["📁"] = "[DIR]",
  ["📂"] = "[DIR]",
  ["🔍"] = "[SEARCH]",
  ["🛠️"] = "[TOOL]",
  ["🛠"] = "[TOOL]",
  ["🔄"] = "[REFRESH]",
  ["📋"] = "[LIST]",
  ["🏗️"] = "[BUILD]",
  ["🏗"] = "[BUILD]",
  ["🟢"] = "[OK]",
  ["📄"] = "[FILE]",
  ["→"] = "->",
  ["📱"] = "[MOBILE]",
  ["🔒"] = "[LOCK]",
  ["🔴"] = "[FAIL]",
  ["🟡"] = "[WARN]",
  ["🤖"] = "[BOT]",
  ["🤷"] = "[SHRUG]",
  ["\xEF\xB8\x8F"] = "",   -- U+FE0F variation selector-16 (zero-width)
}

--- Replace emoji in a string
local function replace_emoji(text)
  for emoji, replacement in pairs(emoji_map) do
    text = text:gsub(emoji, replacement)
  end
  return text
end

function Str(el)
  local new_text = replace_emoji(el.text)
  if new_text ~= el.text then
    return pandoc.Str(new_text)
  end
  return el
end

--- Replace emoji inside inline code spans
function Code(el)
  local new_text = replace_emoji(el.text)
  if new_text ~= el.text then
    el.text = new_text
  end
  return el
end

--- Replace emoji inside fenced code blocks
function CodeBlock(el)
  local new_text = replace_emoji(el.text)
  if new_text ~= el.text then
    el.text = new_text
  end
  return el
end
