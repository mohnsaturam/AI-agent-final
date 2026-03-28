import re
from bs4 import BeautifulSoup, NavigableString, Tag

def html_to_markdown(html_content: str) -> str:
    """
    Convert raw HTML into structured Markdown.
    Specifically designed for AI ingestion: preserves <table>, <tr>, <td> as Markdown tables,
    and <ul>, <ol>, <li> as Markdown lists. Strips all other tags.
    
    Works generically across any site — no site-specific assumptions.
    """
    if not html_content:
        return ""

    try:
        soup = BeautifulSoup(html_content, "html.parser")
    except Exception:
        return ""

    # 1. Remove truly non-content tags (safe to remove on ANY site)
    #    NOTE: 'meta' is intentionally excluded — html.parser treats it as void/self-closing,
    #    and Wikipedia embeds <meta> inside content divs. Decomposing it destroys the parent chain.
    safe_remove_tags = ['script', 'style', 'head', 'title', 'noscript', 'svg', 'iframe', 'form', 'button']
    for element in soup.find_all(safe_remove_tags):
        element.decompose()

    # 2. Remove common ad/tracking/nav junk by class name SUBSTRINGS (generic, works on any site)
    #    These patterns appear across IMDb, Wikipedia, Rotten Tomatoes, etc.
    junk_class_substrings = [
        'navbox', 'sidebar', 'reflist', 'references',           # Wikipedia
        'catlinks', 'printfooter', 'mw-footer', 'mw-editsection',
        'noprint', 'mw-empty-elt', 'sistersitebox',
        'vector-header-container', 'vector-sticky-header',
        'ad-slot', 'ad-container', 'ad-wrapper', 'ad_',         # Generic ads
        'sponsor', 'promoted',                                    # Sponsored content
        'cookie-banner', 'cookie-consent', 'gdpr',              # Cookie banners
        'social-share', 'share-bar', 'share-buttons',           # Social sharing
        'newsletter-signup', 'subscribe-banner',                  # Newsletter popups
        'breadcrumb',                                             # Breadcrumbs (nav, not content)
    ]
    for element in soup.find_all(attrs={"class": True}):
        classes = element.get("class", [])
        class_str = " ".join(classes).lower() if isinstance(classes, list) else str(classes).lower()
        if any(sub in class_str for sub in junk_class_substrings):
            element.decompose()

    # 3. Remove specific junk IDs (generic patterns)
    junk_ids = ['toc', 'catlinks', 'References', 'toc-References',
                'External_links', 'toc-External_links', 'See_also', 'toc-See_also',
                'mw-navigation', 'footer']
    for id_name in junk_ids:
        el = soup.find(id=id_name)
        if el:
            el.decompose()

    # 4. Remove <sup> tags with class 'reference' (Wikipedia footnote markers like [1], [2])
    for sup in soup.find_all('sup', class_='reference'):
        sup.decompose()

    # 5. Remove nav, footer, aside elements (semantic HTML5 non-content tags)
    #    But NOT header — some sites use <header> for content titles
    for element in soup.find_all(['nav', 'footer', 'aside']):
        element.decompose()

    # Helper recursive function
    def process_node(node) -> str:
        if isinstance(node, NavigableString):
            text = str(node).replace('\n', ' ').strip()
            return text + " " if text else ""

        if not isinstance(node, Tag):
            return ""

        tag = node.name.lower()

        # Handle Tables
        if tag == "table":
            rows = node.find_all("tr", recursive=False)
            if not rows:
                # sometimes tr are inside tbody/thead
                rows = node.find_all("tr")
            
            md_table = "\n\n"
            is_first_row = True
            for tr in rows:
                cells = tr.find_all(["th", "td"], recursive=False)
                if not cells:
                    continue
                
                row_text = "| " + " | ".join(process_node(c).strip().replace("|", "\\|") for c in cells) + " |\n"
                md_table += row_text
                
                if is_first_row:
                    md_table += "| " + " | ".join("---" for _ in cells) + " |\n"
                    is_first_row = False
                    
            return md_table + "\n"

        # Handle Lists
        elif tag in ("ul", "ol"):
            md_list = "\n"
            for li in node.find_all("li", recursive=False):
                prefix = "- " if tag == "ul" else "1. "
                md_list += f"{prefix}{process_node(li).strip()}\n"
            return md_list + "\n"

        # Handle Headings
        elif tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            level = int(tag[1])
            return f"\n{'#' * level} {node.get_text(separator=' ', strip=True)}\n\n"

        # Handle Paragraphs and Divs
        elif tag in ("p", "div", "section", "article", "header", "main"):
            content = "".join(process_node(child) for child in node.children).strip()
            return f"\n{content}\n" if content else ""

        # Handle Line Breaks
        elif tag == "br":
            return "\n"

        # Handle Links
        elif tag == "a":
            text = "".join(process_node(child) for child in node.children).strip()
            # We skip adding the actual URL for AI consumption to save tokens, the text is usually enough
            return f" [{text}] " if text else ""

        # Handle Images — preserve alt text as content (useful for title discovery on lazy-loaded pages)
        elif tag == "img":
            alt = node.get("alt", "").strip()
            return f" {alt} " if alt else ""

        # Recursive for inline tags (span, strong, em, etc.)
        else:
            return "".join(process_node(child) for child in node.children)

    # Process body or root
    body = soup.find("body") or soup
    raw_md = process_node(body)

    # Cleanup multiple newlines
    clean_md = re.sub(r'\n{3,}', '\n\n', raw_md).strip()
    return clean_md

