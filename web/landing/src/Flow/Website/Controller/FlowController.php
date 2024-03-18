<?php

declare(strict_types=1);

namespace Flow\Website\Controller;

use Flow\Website\Service\Github;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class FlowController extends AbstractController
{
    public function __construct(private readonly Github $github)
    {
    }

    #[Route('/changelog', name: 'changelog', priority: 100)]
    public function main() : Response
    {
        return $this->render('main/changelog.html.twig', [
            'changelog_markdown' => \file_get_contents($this->getParameter('flow_root_dir') . '/CHANGELOG.md'),
        ]);
    }
}