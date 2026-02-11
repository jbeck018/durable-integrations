// Package whitelabel provides white-label configuration and theming for
// embedded integration UIs. It generates CSS themes and branding assets
// from a declarative configuration.
package whitelabel

import (
	"fmt"
	"strings"
)

// WhiteLabelConfig holds the branding configuration for a tenant's embedded
// integration UI.
type WhiteLabelConfig struct {
	Logo           string `json:"logo"`
	PrimaryColor   string `json:"primary_color"`
	SecondaryColor string `json:"secondary_color"`
	CompanyName    string `json:"company_name"`
	CustomCSS      string `json:"custom_css,omitempty"`
	CustomDomain   string `json:"custom_domain,omitempty"`
	FontFamily     string `json:"font_family,omitempty"`
	BorderRadius   string `json:"border_radius,omitempty"`
	DarkMode       bool   `json:"dark_mode,omitempty"`
}

// Theme is the generated theme output from a WhiteLabelConfig. It contains
// CSS custom properties, branding URLs, and optional custom CSS overrides.
type Theme struct {
	CSSVariables map[string]string `json:"css_variables"`
	LogoURL      string            `json:"logo_url"`
	CompanyName  string            `json:"company_name"`
	CustomCSS    string            `json:"custom_css,omitempty"`
	CustomDomain string            `json:"custom_domain,omitempty"`
	GeneratedCSS string            `json:"generated_css"`
}

// ThemeGenerator produces Theme objects from WhiteLabelConfig inputs.
type ThemeGenerator struct {
	// DefaultPrimaryColor is the fallback primary color when not configured.
	DefaultPrimaryColor string
	// DefaultSecondaryColor is the fallback secondary color.
	DefaultSecondaryColor string
	// DefaultFontFamily is the fallback font stack.
	DefaultFontFamily string
	// DefaultBorderRadius is the fallback border radius.
	DefaultBorderRadius string
}

// NewThemeGenerator creates a ThemeGenerator with sensible defaults.
func NewThemeGenerator() *ThemeGenerator {
	return &ThemeGenerator{
		DefaultPrimaryColor:   "#4F46E5",
		DefaultSecondaryColor: "#7C3AED",
		DefaultFontFamily:     "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
		DefaultBorderRadius:   "8px",
	}
}

// GenerateTheme produces a complete Theme from the given configuration,
// applying defaults for any unconfigured values.
func (tg *ThemeGenerator) GenerateTheme(config WhiteLabelConfig) *Theme {
	primary := config.PrimaryColor
	if primary == "" {
		primary = tg.DefaultPrimaryColor
	}
	secondary := config.SecondaryColor
	if secondary == "" {
		secondary = tg.DefaultSecondaryColor
	}
	fontFamily := config.FontFamily
	if fontFamily == "" {
		fontFamily = tg.DefaultFontFamily
	}
	borderRadius := config.BorderRadius
	if borderRadius == "" {
		borderRadius = tg.DefaultBorderRadius
	}

	// Build CSS variables map.
	vars := map[string]string{
		"--ff-primary":          primary,
		"--ff-primary-hover":    adjustColor(primary, -10),
		"--ff-primary-light":    adjustColor(primary, 40),
		"--ff-secondary":        secondary,
		"--ff-secondary-hover":  adjustColor(secondary, -10),
		"--ff-secondary-light":  adjustColor(secondary, 40),
		"--ff-font-family":      fontFamily,
		"--ff-border-radius":    borderRadius,
		"--ff-border-radius-sm": adjustRadius(borderRadius, 0.5),
		"--ff-border-radius-lg": adjustRadius(borderRadius, 1.5),
	}

	if config.DarkMode {
		vars["--ff-bg-primary"] = "#1a1a2e"
		vars["--ff-bg-secondary"] = "#16213e"
		vars["--ff-bg-surface"] = "#0f3460"
		vars["--ff-text-primary"] = "#e6e6e6"
		vars["--ff-text-secondary"] = "#a6a6a6"
		vars["--ff-border-color"] = "#2a2a4a"
	} else {
		vars["--ff-bg-primary"] = "#ffffff"
		vars["--ff-bg-secondary"] = "#f9fafb"
		vars["--ff-bg-surface"] = "#f3f4f6"
		vars["--ff-text-primary"] = "#111827"
		vars["--ff-text-secondary"] = "#6b7280"
		vars["--ff-border-color"] = "#e5e7eb"
	}

	// Generate CSS string.
	css := generateCSS(vars, config.CustomCSS)

	return &Theme{
		CSSVariables: vars,
		LogoURL:      config.Logo,
		CompanyName:  config.CompanyName,
		CustomCSS:    config.CustomCSS,
		CustomDomain: config.CustomDomain,
		GeneratedCSS: css,
	}
}

// Validate checks a WhiteLabelConfig for common errors.
func Validate(config WhiteLabelConfig) []string {
	var errors []string

	if config.PrimaryColor != "" && !isValidColor(config.PrimaryColor) {
		errors = append(errors, fmt.Sprintf("invalid primary color: %q", config.PrimaryColor))
	}
	if config.SecondaryColor != "" && !isValidColor(config.SecondaryColor) {
		errors = append(errors, fmt.Sprintf("invalid secondary color: %q", config.SecondaryColor))
	}
	if config.CustomDomain != "" {
		if strings.Contains(config.CustomDomain, " ") || !strings.Contains(config.CustomDomain, ".") {
			errors = append(errors, fmt.Sprintf("invalid custom domain: %q", config.CustomDomain))
		}
	}
	if config.Logo != "" {
		if !strings.HasPrefix(config.Logo, "http://") && !strings.HasPrefix(config.Logo, "https://") && !strings.HasPrefix(config.Logo, "/") && !strings.HasPrefix(config.Logo, "data:") {
			errors = append(errors, fmt.Sprintf("logo must be a URL or data URI: %q", config.Logo))
		}
	}
	return errors
}

// generateCSS builds the complete CSS string from variables and custom CSS.
func generateCSS(vars map[string]string, customCSS string) string {
	var sb strings.Builder

	sb.WriteString(":root {\n")
	// Sort keys for deterministic output.
	keys := sortedKeys(vars)
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("  %s: %s;\n", k, vars[k]))
	}
	sb.WriteString("}\n\n")

	// Base component styles using CSS variables.
	sb.WriteString(`.ff-container {
  font-family: var(--ff-font-family);
  background-color: var(--ff-bg-primary);
  color: var(--ff-text-primary);
}

.ff-card {
  background-color: var(--ff-bg-surface);
  border: 1px solid var(--ff-border-color);
  border-radius: var(--ff-border-radius);
  padding: 1.5rem;
}

.ff-button-primary {
  background-color: var(--ff-primary);
  color: #ffffff;
  border: none;
  border-radius: var(--ff-border-radius-sm);
  padding: 0.5rem 1rem;
  cursor: pointer;
  font-family: var(--ff-font-family);
}

.ff-button-primary:hover {
  background-color: var(--ff-primary-hover);
}

.ff-button-secondary {
  background-color: var(--ff-secondary);
  color: #ffffff;
  border: none;
  border-radius: var(--ff-border-radius-sm);
  padding: 0.5rem 1rem;
  cursor: pointer;
  font-family: var(--ff-font-family);
}

.ff-button-secondary:hover {
  background-color: var(--ff-secondary-hover);
}

.ff-input {
  border: 1px solid var(--ff-border-color);
  border-radius: var(--ff-border-radius-sm);
  padding: 0.5rem 0.75rem;
  font-family: var(--ff-font-family);
  background-color: var(--ff-bg-primary);
  color: var(--ff-text-primary);
}

.ff-text-secondary {
  color: var(--ff-text-secondary);
}
`)

	if customCSS != "" {
		sb.WriteString("\n/* Custom CSS overrides */\n")
		sb.WriteString(customCSS)
		sb.WriteString("\n")
	}

	return sb.String()
}

// isValidColor checks if a string looks like a valid CSS color.
func isValidColor(color string) bool {
	if strings.HasPrefix(color, "#") {
		hex := color[1:]
		if len(hex) != 3 && len(hex) != 6 && len(hex) != 8 {
			return false
		}
		for _, c := range hex {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return false
			}
		}
		return true
	}
	if strings.HasPrefix(color, "rgb") || strings.HasPrefix(color, "hsl") {
		return true
	}
	// Named colors.
	namedColors := map[string]bool{
		"red": true, "blue": true, "green": true, "black": true,
		"white": true, "gray": true, "grey": true, "orange": true,
		"yellow": true, "purple": true, "pink": true, "brown": true,
		"transparent": true, "inherit": true, "currentColor": true,
	}
	return namedColors[strings.ToLower(color)]
}

// adjustColor lightens or darkens a hex color by the given percentage.
// Positive values lighten, negative values darken.
func adjustColor(hex string, percent int) string {
	if !strings.HasPrefix(hex, "#") || len(hex) != 7 {
		return hex
	}

	r := hexToInt(hex[1:3])
	g := hexToInt(hex[3:5])
	b := hexToInt(hex[5:7])

	if percent > 0 {
		r = r + (255-r)*percent/100
		g = g + (255-g)*percent/100
		b = b + (255-b)*percent/100
	} else {
		factor := 100 + percent
		r = r * factor / 100
		g = g * factor / 100
		b = b * factor / 100
	}

	r = clamp(r, 0, 255)
	g = clamp(g, 0, 255)
	b = clamp(b, 0, 255)

	return fmt.Sprintf("#%02x%02x%02x", r, g, b)
}

// adjustRadius scales a CSS border-radius value by a factor.
func adjustRadius(radius string, factor float64) string {
	// Parse the numeric part.
	var value float64
	var unit string
	n, _ := fmt.Sscanf(radius, "%f", &value)
	if n == 0 {
		return radius
	}
	// Extract the unit suffix.
	for i, c := range radius {
		if (c < '0' || c > '9') && c != '.' {
			unit = radius[i:]
			break
		}
	}
	if unit == "" {
		unit = "px"
	}
	return fmt.Sprintf("%.0f%s", value*factor, unit)
}

func hexToInt(s string) int {
	val := 0
	for _, c := range s {
		val *= 16
		switch {
		case c >= '0' && c <= '9':
			val += int(c - '0')
		case c >= 'a' && c <= 'f':
			val += int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			val += int(c-'A') + 10
		}
	}
	return val
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// sortedKeys returns map keys in sorted order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort for small maps.
	for i := 1; i < len(keys); i++ {
		j := i
		for j > 0 && keys[j] < keys[j-1] {
			keys[j], keys[j-1] = keys[j-1], keys[j]
			j--
		}
	}
	return keys
}
