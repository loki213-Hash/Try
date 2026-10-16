"""Black-Scholes Options Pricing and Greeks Engine.

Pure Python analytical implementation using standard Gaussian distribution.
Provides Delta, Gamma, Theta, Vega, and theoretical option valuation
to project realistic option price movement for targets without fabrication.
"""

from __future__ import annotations
import math
from typing import Optional, Tuple
from app.data.base_provider import OptionGreeks


def normal_cdf(x: float) -> float:
    """Standard normal cumulative distribution function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def normal_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * x * x)


class BlackScholesGreeks:
    """Calculates theoretical prices and sensitivities (Greeks)."""

    @staticmethod
    def calculate_greeks(
        spot: float,
        strike: float,
        time_to_expiry_years: float,
        volatility: float,  # Annualized (e.g. 0.15 for 15%)
        risk_free_rate: float = 0.065,  # Indian 10y benchmark ~6.5%
        is_call: bool = True,
    ) -> OptionGreeks:
        """Returns Delta, Gamma, Theta, Vega, IV using Black-Scholes formula."""
        if time_to_expiry_years <= 0 or volatility <= 0 or spot <= 0 or strike <= 0:
            delta = 1.0 if (is_call and spot > strike) else (0.0 if is_call else -1.0)
            return OptionGreeks(delta=delta, theta=0.0, gamma=0.0, vega=0.0, iv=volatility * 100)

        t = max(time_to_expiry_years, 0.0001)
        r = risk_free_rate
        v = volatility

        d1 = (math.log(spot / strike) + (r + 0.5 * v * v) * t) / (v * math.sqrt(t))
        d2 = d1 - v * math.sqrt(t)

        pdf_d1 = normal_pdf(d1)
        cdf_d1 = normal_cdf(d1)
        cdf_d2 = normal_cdf(d2)

        gamma = pdf_d1 / (spot * v * math.sqrt(t))
        vega = (spot * math.sqrt(t) * pdf_d1) / 100.0  # Per 1% change in vol

        if is_call:
            delta = cdf_d1
            theta = (
                -(spot * pdf_d1 * v) / (2.0 * math.sqrt(t))
                - r * strike * math.exp(-r * t) * cdf_d2
            ) / 365.0  # Per calendar day
        else:
            delta = cdf_d1 - 1.0
            theta = (
                -(spot * pdf_d1 * v) / (2.0 * math.sqrt(t))
                + r * strike * math.exp(-r * t) * normal_cdf(-d2)
            ) / 365.0

        return OptionGreeks(
            delta=round(delta, 3),
            gamma=round(gamma, 5),
            theta=round(theta, 2),
            vega=round(vega, 2),
            iv=round(volatility * 100.0, 2),
        )

    @classmethod
    def project_option_price(
        cls,
        current_premium: float,
        spot_move: float,
        delta: float,
        gamma: float = 0.0,
    ) -> float:
        """Projects expected option premium from an underlying index move using Taylor expansion.
        
        dPremium = Delta * dSpot + 0.5 * Gamma * (dSpot)^2
        """
        linear_component = delta * spot_move
        convexity_component = 0.5 * gamma * (spot_move ** 2)
        expected_change = linear_component + convexity_component
        new_premium = max(1.0, current_premium + expected_change)
        return round(new_premium, 2)
