/**
 * ConnectorCard — renders an individual connector as a selectable card.
 * Shows icon, name, description, type badge, and status indicator.
 */

import React, { useCallback } from "react";
import type { Connector, ConnectorType } from "../../lib/types";

// ---------------------------------------------------------------------------
// Styles (CSS-in-JS objects for zero-dependency styling)
// ---------------------------------------------------------------------------

const styles = {
  card: (isSelected: boolean): React.CSSProperties => ({
    display: "flex",
    flexDirection: "column",
    gap: "12px",
    padding: "16px",
    border: `2px solid ${isSelected ? "#3b82f6" : "#e5e7eb"}`,
    borderRadius: "12px",
    background: isSelected ? "#eff6ff" : "#ffffff",
    cursor: "pointer",
    transition: "all 0.15s ease",
    position: "relative",
    overflow: "hidden",
  }),
  cardHover: {
    borderColor: "#93c5fd",
    boxShadow: "0 4px 12px rgba(59, 130, 246, 0.15)",
  } as React.CSSProperties,
  header: {
    display: "flex",
    alignItems: "center",
    gap: "12px",
  } as React.CSSProperties,
  iconContainer: {
    width: "40px",
    height: "40px",
    borderRadius: "8px",
    background: "#f3f4f6",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    flexShrink: 0,
    fontSize: "20px",
  } as React.CSSProperties,
  nameRow: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "2px",
    minWidth: 0,
    flex: 1,
  } as React.CSSProperties,
  name: {
    fontSize: "14px",
    fontWeight: 600,
    color: "#111827",
    whiteSpace: "nowrap" as const,
    overflow: "hidden",
    textOverflow: "ellipsis",
    margin: 0,
  } as React.CSSProperties,
  version: {
    fontSize: "11px",
    color: "#9ca3af",
    margin: 0,
  } as React.CSSProperties,
  description: {
    fontSize: "13px",
    color: "#6b7280",
    lineHeight: "1.4",
    display: "-webkit-box",
    WebkitLineClamp: 2,
    WebkitBoxOrient: "vertical" as const,
    overflow: "hidden",
    margin: 0,
  } as React.CSSProperties,
  footer: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: "8px",
  } as React.CSSProperties,
  badge: (type: ConnectorType): React.CSSProperties => {
    const colors: Record<ConnectorType, { bg: string; text: string }> = {
      source: { bg: "#dcfce7", text: "#166534" },
      destination: { bg: "#dbeafe", text: "#1e40af" },
      bidirectional: { bg: "#fef3c7", text: "#92400e" },
    };
    const c = colors[type] ?? { bg: "#f3f4f6", text: "#374151" };
    return {
      fontSize: "11px",
      fontWeight: 600,
      padding: "2px 8px",
      borderRadius: "9999px",
      background: c.bg,
      color: c.text,
      textTransform: "capitalize",
      whiteSpace: "nowrap",
    };
  },
  categoryTag: {
    fontSize: "11px",
    color: "#9ca3af",
    whiteSpace: "nowrap" as const,
    overflow: "hidden",
    textOverflow: "ellipsis",
  } as React.CSSProperties,
  selectedIndicator: {
    position: "absolute" as const,
    top: "8px",
    right: "8px",
    width: "20px",
    height: "20px",
    borderRadius: "50%",
    background: "#3b82f6",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    color: "#ffffff",
    fontSize: "12px",
    fontWeight: 700,
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface ConnectorCardProps {
  connector: Connector;
  isSelected: boolean;
  onClick: (connector: Connector) => void;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const ConnectorCardInner: React.FC<ConnectorCardProps> = ({
  connector,
  isSelected,
  onClick,
}) => {
  const [hovered, setHovered] = React.useState(false);

  const handleClick = useCallback(() => {
    onClick(connector);
  }, [onClick, connector]);

  const handleMouseEnter = useCallback(() => setHovered(true), []);
  const handleMouseLeave = useCallback(() => setHovered(false), []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onClick(connector);
      }
    },
    [onClick, connector],
  );

  const cardStyle: React.CSSProperties = {
    ...styles.card(isSelected),
    ...(hovered && !isSelected ? styles.cardHover : {}),
  };

  const fallbackIcon = connector.type === "source" ? "\u2B06" : connector.type === "destination" ? "\u2B07" : "\u21C6";

  return (
    <div
      role="button"
      tabIndex={0}
      style={cardStyle}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      aria-selected={isSelected}
      aria-label={`${connector.display_name || connector.name} connector`}
    >
      {isSelected && (
        <div style={styles.selectedIndicator} aria-hidden="true">
          &#10003;
        </div>
      )}

      <div style={styles.header}>
        <div style={styles.iconContainer} aria-hidden="true">
          {connector.icon ? (
            <img
              src={connector.icon}
              alt=""
              width={24}
              height={24}
              style={{ objectFit: "contain" }}
              onError={(e) => {
                (e.target as HTMLImageElement).style.display = "none";
                const parent = (e.target as HTMLImageElement).parentElement;
                if (parent) parent.textContent = fallbackIcon;
              }}
            />
          ) : (
            fallbackIcon
          )}
        </div>
        <div style={styles.nameRow}>
          <p style={styles.name}>{connector.display_name || connector.name}</p>
          {connector.version && (
            <p style={styles.version}>v{connector.version}</p>
          )}
        </div>
      </div>

      {connector.description && (
        <p style={styles.description}>{connector.description}</p>
      )}

      <div style={styles.footer}>
        <span style={styles.badge(connector.type)}>{connector.type}</span>
        {connector.category && (
          <span style={styles.categoryTag}>{connector.category}</span>
        )}
      </div>
    </div>
  );
};

export const ConnectorCard = React.memo(ConnectorCardInner);
