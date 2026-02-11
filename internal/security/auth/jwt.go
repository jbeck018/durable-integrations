package auth

import (
	"crypto/ecdsa"
	"crypto/rsa"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims contains the FlowForge-specific JWT payload.
type Claims struct {
	PrincipalID string   `json:"pid"`
	TenantID    string   `json:"tid"`
	UserID      string   `json:"uid"`
	Scopes      []string `json:"scopes"`
	jwt.RegisteredClaims
}

// JWTConfig holds the configuration for the JWT service.
type JWTConfig struct {
	// SigningMethod is the JWT algorithm. Supported: HS256, RS256, ES256.
	SigningMethod jwt.SigningMethod

	// HMACSecret is used when SigningMethod is HS256.
	HMACSecret []byte

	// RSAPrivateKey and RSAPublicKey are used when SigningMethod is RS256.
	RSAPrivateKey *rsa.PrivateKey
	RSAPublicKey  *rsa.PublicKey

	// ECDSAPrivateKey and ECDSAPublicKey are used when SigningMethod is ES256.
	ECDSAPrivateKey *ecdsa.PrivateKey
	ECDSAPublicKey  *ecdsa.PublicKey

	// Issuer is embedded in the "iss" claim.
	Issuer string

	// Audience is embedded in the "aud" claim.
	Audience []string
}

// JWTService handles JWT token generation and validation.
type JWTService struct {
	config JWTConfig
}

// NewJWTService creates a JWTService from the provided configuration.
// It validates that the required keys are present for the chosen signing method.
func NewJWTService(config JWTConfig) (*JWTService, error) {
	if config.SigningMethod == nil {
		return nil, errors.New("signing method is required")
	}

	switch config.SigningMethod.Alg() {
	case "HS256", "HS384", "HS512":
		if len(config.HMACSecret) < 32 {
			return nil, errors.New("HMAC secret must be at least 32 bytes")
		}
	case "RS256", "RS384", "RS512":
		if config.RSAPrivateKey == nil {
			return nil, errors.New("RSA private key is required for RS256 signing")
		}
		if config.RSAPublicKey == nil {
			config.RSAPublicKey = &config.RSAPrivateKey.PublicKey
		}
	case "ES256", "ES384", "ES512":
		if config.ECDSAPrivateKey == nil {
			return nil, errors.New("ECDSA private key is required for ES256 signing")
		}
		if config.ECDSAPublicKey == nil {
			config.ECDSAPublicKey = &config.ECDSAPrivateKey.PublicKey
		}
	default:
		return nil, fmt.Errorf("unsupported signing method: %s", config.SigningMethod.Alg())
	}

	if config.Issuer == "" {
		config.Issuer = "flowforge"
	}

	return &JWTService{config: config}, nil
}

// NewHMACJWTService is a convenience constructor for HS256 JWTs.
func NewHMACJWTService(secret []byte) (*JWTService, error) {
	return NewJWTService(JWTConfig{
		SigningMethod: jwt.SigningMethodHS256,
		HMACSecret:   secret,
		Issuer:       "flowforge",
	})
}

// NewRSAJWTService is a convenience constructor for RS256 JWTs.
func NewRSAJWTService(privateKey *rsa.PrivateKey) (*JWTService, error) {
	return NewJWTService(JWTConfig{
		SigningMethod: jwt.SigningMethodRS256,
		RSAPrivateKey: privateKey,
		RSAPublicKey:  &privateKey.PublicKey,
		Issuer:        "flowforge",
	})
}

// GenerateToken creates a signed JWT for the given principal with the specified TTL.
func (s *JWTService) GenerateToken(principal *Principal, ttl time.Duration) (string, error) {
	if principal == nil {
		return "", errors.New("principal is required")
	}
	if ttl <= 0 {
		return "", errors.New("TTL must be positive")
	}

	now := time.Now().UTC()
	claims := Claims{
		PrincipalID: principal.ID,
		TenantID:    principal.TenantID,
		UserID:      principal.UserID,
		Scopes:      principal.Scopes,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.config.Issuer,
			Subject:   principal.UserID,
			Audience:  jwt.ClaimStrings(s.config.Audience),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ID:        principal.ID,
		},
	}

	token := jwt.NewWithClaims(s.config.SigningMethod, claims)
	signingKey, err := s.signingKey()
	if err != nil {
		return "", fmt.Errorf("getting signing key: %w", err)
	}

	tokenStr, err := token.SignedString(signingKey)
	if err != nil {
		return "", fmt.Errorf("signing JWT: %w", err)
	}

	return tokenStr, nil
}

// ValidateToken parses and validates a JWT string, returning the embedded claims.
func (s *JWTService) ValidateToken(tokenStr string) (*Claims, error) {
	if tokenStr == "" {
		return nil, errors.New("token string is empty")
	}

	parserOpts := []jwt.ParserOption{
		jwt.WithIssuer(s.config.Issuer),
		jwt.WithValidMethods([]string{s.config.SigningMethod.Alg()}),
		jwt.WithExpirationRequired(),
	}
	if len(s.config.Audience) > 0 {
		parserOpts = append(parserOpts, jwt.WithAudience(s.config.Audience[0]))
	}

	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		// Verify the signing algorithm matches the expected method.
		if t.Method.Alg() != s.config.SigningMethod.Alg() {
			return nil, fmt.Errorf("unexpected signing method: %s", t.Header["alg"])
		}
		return s.verificationKey()
	}, parserOpts...)

	if err != nil {
		return nil, fmt.Errorf("parsing JWT: %w", err)
	}

	if !token.Valid {
		return nil, errors.New("token is invalid")
	}

	if claims.PrincipalID == "" {
		return nil, errors.New("token missing principal ID")
	}
	if claims.TenantID == "" {
		return nil, errors.New("token missing tenant ID")
	}

	return claims, nil
}

// signingKey returns the key used to sign tokens based on the signing method.
func (s *JWTService) signingKey() (interface{}, error) {
	switch s.config.SigningMethod.Alg() {
	case "HS256", "HS384", "HS512":
		return s.config.HMACSecret, nil
	case "RS256", "RS384", "RS512":
		if s.config.RSAPrivateKey == nil {
			return nil, errors.New("RSA private key not configured")
		}
		return s.config.RSAPrivateKey, nil
	case "ES256", "ES384", "ES512":
		if s.config.ECDSAPrivateKey == nil {
			return nil, errors.New("ECDSA private key not configured")
		}
		return s.config.ECDSAPrivateKey, nil
	default:
		return nil, fmt.Errorf("unsupported signing method: %s", s.config.SigningMethod.Alg())
	}
}

// verificationKey returns the key used to verify tokens based on the signing method.
func (s *JWTService) verificationKey() (interface{}, error) {
	switch s.config.SigningMethod.Alg() {
	case "HS256", "HS384", "HS512":
		return s.config.HMACSecret, nil
	case "RS256", "RS384", "RS512":
		if s.config.RSAPublicKey == nil {
			return nil, errors.New("RSA public key not configured")
		}
		return s.config.RSAPublicKey, nil
	case "ES256", "ES384", "ES512":
		if s.config.ECDSAPublicKey == nil {
			return nil, errors.New("ECDSA public key not configured")
		}
		return s.config.ECDSAPublicKey, nil
	default:
		return nil, fmt.Errorf("unsupported signing method: %s", s.config.SigningMethod.Alg())
	}
}
