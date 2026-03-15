-- mermaid-render.lua
-- Pandoc Lua filter that renders ```mermaid code blocks to PNG images
-- using the @mermaid-js/mermaid-cli (mmdc) tool.
--
-- Requires: npm install -g @mermaid-js/mermaid-cli
--
-- Each mermaid block is rendered to a PNG in build/mermaid-images/
-- and replaced with an Image element in the document.

local counter = 0
local build_dir = nil

--- Locate mmdc executable
local function find_mmdc()
  local handle = io.popen("which mmdc 2>/dev/null")
  if handle then
    local result = handle:read("*l")
    handle:close()
    if result and result ~= "" then
      return result
    end
  end
  return nil
end

local mmdc_path = find_mmdc()

--- Ensure the output directory exists
local function ensure_dir(dir)
  os.execute('mkdir -p "' .. dir .. '"')
end

--- Get the build directory for mermaid images
local function get_image_dir()
  if build_dir then return build_dir end
  -- Use MERMAID_IMG_DIR env var or default to build/mermaid-images
  local env_dir = os.getenv("MERMAID_IMG_DIR")
  if env_dir then
    build_dir = env_dir
  else
    build_dir = "build/mermaid-images"
  end
  ensure_dir(build_dir)
  return build_dir
end

function CodeBlock(el)
  -- Only process mermaid code blocks
  if not el.classes:includes("mermaid") then
    return el
  end

  -- Skip if mmdc is not available
  if not mmdc_path then
    return el
  end

  counter = counter + 1
  local img_dir = get_image_dir()
  local basename = string.format("mermaid-%03d", counter)
  local input_file = img_dir .. "/" .. basename .. ".mmd"
  local output_file = img_dir .. "/" .. basename .. ".png"

  -- Write mermaid source to temp file
  local f = io.open(input_file, "w")
  if not f then
    io.stderr:write("WARNING: Could not write mermaid file: " .. input_file .. "\n")
    return el
  end
  f:write(el.text)
  f:close()

  -- Render with mmdc
  local cmd = string.format(
    '%s -i "%s" -o "%s" -w 1200 -b white --quiet 2>&1',
    mmdc_path, input_file, output_file
  )
  local handle = io.popen(cmd)
  local result = handle:read("*a")
  local success = handle:close()

  -- Check if output was created
  local check = io.open(output_file, "r")
  if check then
    check:close()
    -- Use RawBlock with LaTeX to include the image with explicit width control
    -- This avoids pandoc's image handling which can cause page overflow
    local abs_path = output_file
    -- If path is relative, get absolute path
    if not abs_path:match("^/") then
      local h = io.popen("cd '" .. (os.getenv("MERMAID_IMG_DIR") or ".") .. "/..' && pwd")
      if h then
        local base = h:read("*l")
        h:close()
        if base then
          abs_path = base .. "/" .. output_file
        end
      end
    end
    local latex = string.format(
      "\\begin{center}\n\\includegraphics[width=\\textwidth,height=0.85\\textheight,keepaspectratio]{%s}\n\\end{center}",
      abs_path
    )
    return pandoc.RawBlock("latex", latex)
  else
    io.stderr:write("WARNING: Mermaid render failed for block " .. counter .. ": " .. result .. "\n")
    return el  -- keep as code block
  end
end
